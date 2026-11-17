package xplat.flow.plugin

import cats.effect.kernel.{Async, Deferred, Outcome, Ref, Resource}
import cats.effect.std.Mutex
import cats.syntax.all._
import io.circe.Json
import io.circe.parser.parse

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/** A process that answers JSON-RPC 2.0, one message per line: requests go to its stdin and answers come from its stdout. What it
  * writes to stderr is its log. Several requests can be pending at once; each answer finds its request by `id`.
  *
  * When the process ends, the requests that were pending fail and so does every later one: [[RpcProcess.Supervised]] starts
  * another.
  */
final class RpcProcess[F[_]] private (
    process: Process,
    writer: BufferedWriter,
    writeLock: Mutex[F],
    nextId: Ref[F, Long],
    pending: Ref[F, Map[Long, Deferred[F, Either[String, Json]]]],
    ended: Ref[F, Option[String]]
)(implicit F: Async[F]) {

  /** Whether the process is still there to answer. */
  def alive: F[Boolean] = ended.get.map(_.isEmpty)

  /** Sends `method` with `params` and waits for its answer: the `result`, or the `error`'s message. If the caller is cancelled
    * (a node that times out), the request is forgotten and the process is told with `$/cancel`.
    */
  def call(method: String, params: Json): F[Either[String, Json]] =
    ended.get.flatMap {
      case Some(why) => F.pure(Left(why))
      case None =>
        for {
          id <- nextId.getAndUpdate(_ + 1)
          answer <- Deferred[F, Either[String, Json]]
          _ <- pending.update(_ + (id -> answer))
          request = Json.obj("jsonrpc" -> Json.fromString("2.0"), "id" -> Json.fromLong(id), "method" -> Json.fromString(method), "params" -> params)
          result <- F.guaranteeCase(send(request) >> answer.get) {
            case Outcome.Succeeded(_) => F.unit
            case _                    => pending.update(_ - id) >> notify("$/cancel", Json.obj("id" -> Json.fromLong(id))).attempt.void
          }
        } yield result
    }

  /** Sends a message that has no answer. */
  def notify(method: String, params: Json): F[Unit] =
    send(Json.obj("jsonrpc" -> Json.fromString("2.0"), "method" -> Json.fromString(method), "params" -> params))

  private def send(message: Json): F[Unit] =
    writeLock.lock.surround(F.blocking {
      writer.write(message.noSpaces)
      writer.newLine()
      writer.flush()
    }).handleErrorWith(e => end(s"could not write to the plugin's process: ${e.getMessage}"))

  /** Reads answers until stdout closes. */
  private def readAnswers(reader: BufferedReader, log: String => F[Unit]): F[Unit] =
    F.blocking(Option(reader.readLine())).flatMap {
      case None => F.blocking(process.waitFor(5, TimeUnit.SECONDS)).attempt >> end(s"the plugin's process ended${exitCode.fold("")(c => s" (exit code $c)")}")
      case Some(line) if line.trim.isEmpty => readAnswers(reader, log)
      case Some(line) => answer(line, log) >> readAnswers(reader, log)
    }

  private def exitCode: Option[Int] = if (process.isAlive) None else Some(process.exitValue())

  private def answer(line: String, log: String => F[Unit]): F[Unit] =
    parse(line).toOption.flatMap(j => j.hcursor.get[Long]("id").toOption.map(_ -> j)) match {
      case None => log(s"not a JSON-RPC answer on stdout (use stderr to log): $line")
      case Some((id, j)) =>
        val result = j.hcursor.downField("error").focus match {
          case Some(e) if !e.isNull => Left(e.hcursor.get[String]("message").getOrElse(e.noSpaces))
          case _                    => Right(j.hcursor.downField("result").focus.getOrElse(Json.Null))
        }
        pending.modify(m => (m - id, m.get(id))).flatMap(_.traverse_(_.complete(result).void))
    }

  private def end(why: String): F[Unit] =
    ended.update(_.orElse(Some(why))) >> ended.get.flatMap { reason =>
      pending.getAndSet(Map.empty).flatMap(_.values.toList.traverse_(_.complete(Left(reason.getOrElse(why))).void))
    }

  /** Asks the process to stop, and kills it if it does not within `grace`. */
  private def stop: F[Unit] =
    notify("shutdown", Json.obj()).attempt >>
      F.blocking {
        writer.close()
        if (!process.waitFor(3, TimeUnit.SECONDS)) {
          process.destroy()
          if (!process.waitFor(2, TimeUnit.SECONDS)) process.destroyForcibly()
        }
      }.attempt.void >> end("the plugin's process was stopped")
}

object RpcProcess {

  /** Starts `command` in `dir` (with `env` added to the environment); `log` gets every line of its stderr. */
  def start[F[_]](command: List[String], dir: Path, env: Map[String, String], log: String => F[Unit])(implicit F: Async[F]): Resource[F, RpcProcess[F]] =
    for {
      process <- Resource.make(F.blocking {
        val pb = new ProcessBuilder(command: _*).directory(dir.toFile)
        pb.environment().putAll(scala.jdk.CollectionConverters.MapHasAsJava(env).asJava)
        pb.start()
      })(p => F.blocking { if (p.isAlive) p.destroyForcibly(); () })
      writeLock <- Resource.eval(Mutex[F])
      nextId <- Resource.eval(Ref.of[F, Long](1L))
      pending <- Resource.eval(Ref.of[F, Map[Long, Deferred[F, Either[String, Json]]]](Map.empty))
      ended <- Resource.eval(Ref.of[F, Option[String]](None))
      writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream, UTF_8))
      rpc = new RpcProcess[F](process, writer, writeLock, nextId, pending, ended)
      _ <- F.background(rpc.readAnswers(new BufferedReader(new InputStreamReader(process.getInputStream, UTF_8)), log))
      _ <- F.background(lines(new BufferedReader(new InputStreamReader(process.getErrorStream, UTF_8)), log))
      _ <- Resource.onFinalize(rpc.stop)
    } yield rpc

  private def lines[F[_]](reader: BufferedReader, log: String => F[Unit])(implicit F: Async[F]): F[Unit] =
    F.blocking(Option(reader.readLine())).flatMap {
      case None       => F.unit
      case Some(line) => log(line) >> lines(reader, log)
    }

  /** A process that is started again, with `init`, when the one before ended: the next call after a crash finds a new one. */
  final class Supervised[F[_]] private (start: Resource[F, RpcProcess[F]], init: RpcProcess[F] => F[Json], lock: Mutex[F], current: Ref[F, Option[(RpcProcess[F], F[Unit])]])(
      implicit F: Async[F]
  ) {

    def call(method: String, params: Json): F[Either[String, Json]] =
      get.attempt.flatMap {
        case Left(e)    => F.pure(Left(s"could not start the plugin's process: ${Option(e.getMessage).getOrElse(e.getClass.getName)}"))
        case Right(rpc) => rpc.call(method, params)
      }

    private def get: F[RpcProcess[F]] =
      lock.lock.surround {
        current.get.flatMap {
          case Some((rpc, release)) =>
            rpc.alive.ifM(F.pure(rpc), release.attempt >> current.set(None) >> fresh.map(_._1))
          case None => fresh.map(_._1)
        }
      }

    /** A new process, already initialized, and what `init` answered. */
    private[RpcProcess] def fresh: F[(RpcProcess[F], Json)] =
      F.uncancelable { poll =>
        poll(start.allocated).flatMap { case (rpc, release) =>
          poll(init(rpc)).onError(_ => release.attempt.void).flatTap(_ => current.set(Some(rpc -> release))).map(rpc -> _)
        }
      }

    private[RpcProcess] def close: F[Unit] = current.getAndSet(None).flatMap(_.traverse_(_._2.attempt.void))
  }

  object Supervised {

    /** The first process is started now, so that a plugin that cannot start fails when it is loaded; with what its `init`
      * answered.
      */
    def apply[F[_]](start: Resource[F, RpcProcess[F]])(init: RpcProcess[F] => F[Json])(implicit F: Async[F]): Resource[F, (Supervised[F], Json)] =
      for {
        lock <- Resource.eval(Mutex[F])
        current <- Resource.eval(Ref.of[F, Option[(RpcProcess[F], F[Unit])]](None))
        s <- Resource.make(F.pure(new Supervised[F](start, init, lock, current)))(_.close)
        first <- Resource.eval(lock.lock.surround(s.fresh.map(_._2)))
      } yield (s, first)
  }
}
