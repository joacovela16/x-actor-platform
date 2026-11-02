package xplat.core.io.cluster.internal

import cats.effect.kernel.{Async, Deferred, Ref}
import cats.effect.std.{Dispatcher, Supervisor}
import cats.effect.syntax.all._
import cats.implicits._
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.error.XError
import xplat.core.api.{Codec, ReplyTo}
import xplat.core.cluster.api.{Address, ClusterRef, RefResolver}

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

/** Delivers messages to endpoints identified by a path, on this node or on others.
  *
  * An endpoint is a plain handler of bytes; decoding and routing to actors is up to whoever registers it. Requests are
  * messages that expect one reply, and also carry the replies to cluster asks.
  */
final class Remoting[F[_]](
    val self: Address,
    sendWire: (Address, Wire) => F[Unit],
    handlers: Ref[F, Map[String, Array[Byte] => F[Unit]]],
    requestHandlers: Ref[F, Map[String, Array[Byte] => F[Array[Byte]]]],
    pending: Ref[F, Map[Long, Deferred[F, Either[Throwable, Array[Byte]]]]],
    nextId: Ref[F, Long],
    supervisor: Supervisor[F],
    log: Log4Cats[F]
)(implicit A: Async[F]) {

  def on(path: String)(handler: Array[Byte] => F[Unit]): F[Unit] = handlers.update(_.updated(path, handler))
  def off(path: String): F[Unit] = handlers.update(_ - path)

  def onRequest(path: String)(handler: Array[Byte] => F[Array[Byte]]): F[Unit] = requestHandlers.update(_.updated(path, handler))
  def offRequest(path: String): F[Unit] = requestHandlers.update(_ - path)

  def send(to: Address, path: String, payload: Array[Byte]): F[Unit] =
    if (to == self) deliver(path, payload) else sendWire(to, Wire.Deliver(path, payload))

  def request(to: Address, path: String, payload: Array[Byte], timeout: FiniteDuration): F[Array[Byte]] =
    expect(timeout) { id =>
      if (to == self) supervisor.supervise(handleRequest(self, id, path, payload)).void
      else sendWire(to, Wire.Request(self, id, path, payload))
    }

  /** Registers a pending reply, runs `send` with its id and waits for the reply. */
  def expect(timeout: FiniteDuration)(send: Long => F[Unit]): F[Array[Byte]] =
    for {
      id     <- nextId.getAndUpdate(_ + 1)
      answer <- Deferred[F, Either[Throwable, Array[Byte]]]
      _      <- pending.update(_.updated(id, answer))
      result <- (send(id) >> answer.get.rethrow.timeout(timeout)).guarantee(pending.update(_ - id))
    } yield result

  /** Sends the reply to a request or an ask that originated on `to`. */
  def reply(to: Address, id: Long, payload: Array[Byte]): F[Unit] =
    if (to == self) complete(id, Right(payload)) else sendWire(to, Wire.Reply(id, payload))

  private def fail(to: Address, id: Long, message: String): F[Unit] =
    if (to == self) complete(id, Left(XError(message))) else sendWire(to, Wire.Failure(id, message))

  private def complete(id: Long, result: Either[Throwable, Array[Byte]]): F[Unit] =
    pending.get.flatMap(_.get(id).traverse_(_.complete(result)))

  private def deliver(path: String, payload: Array[Byte]): F[Unit] =
    handlers.get.flatMap {
      case h if h.contains(path) => h(path)(payload).handleErrorWith(e => log.warn(e)(s"Endpoint $path failed"))
      case _                     => log.debug(s"No endpoint at $path; message dropped")
    }

  private def handleRequest(from: Address, id: Long, path: String, payload: Array[Byte]): F[Unit] =
    requestHandlers.get.map(_.get(path)).flatMap {
      case None => fail(from, id, s"no endpoint at $path on $self")
      case Some(handler) =>
        handler(payload).attempt.flatMap {
          case Right(bytes) => reply(from, id, bytes)
          case Left(e)      => fail(from, id, Option(e.getMessage).getOrElse(e.toString))
        }
    }

  /** Handles a message for this layer. Requests run on their own fiber, since their handlers may wait. */
  def receive(wire: Wire): F[Unit] = wire match {
    case Wire.Deliver(path, payload)            => deliver(path, payload)
    case Wire.Request(from, id, path, payload)  => supervisor.supervise(handleRequest(from, id, path, payload)).void
    case Wire.Reply(id, payload)                => complete(id, Right(payload))
    case Wire.Failure(id, message)              => complete(id, Left(XError(message)))
    case _                                      => A.unit
  }
}

object Remoting {
  def apply[F[_]](self: Address, sendWire: (Address, Wire) => F[Unit], supervisor: Supervisor[F], log: Log4Cats[F])(implicit A: Async[F]): F[Remoting[F]] =
    (
      Ref.of[F, Map[String, Array[Byte] => F[Unit]]](Map.empty),
      Ref.of[F, Map[String, Array[Byte] => F[Array[Byte]]]](Map.empty),
      Ref.of[F, Map[Long, Deferred[F, Either[Throwable, Array[Byte]]]]](Map.empty),
      Ref.of[F, Long](0L)
    ).mapN(new Remoting[F](self, sendWire, _, _, _, _, supervisor, log))
}

/** Reply ref of a cluster ask: the reply is encoded and sent to the node that asked, which matches it by `id`. */
final case class ReplyRef[A](address: Address, id: Long)(deliver: A => Unit) extends ReplyTo[A] {
  def send(msg: A): Unit = deliver(msg)
}

final class IORefResolver[F[_]](remoting: Remoting[F], dispatcher: Dispatcher[F], log: Log4Cats[F]) extends RefResolver[F] {
  import IORefResolver._

  def serialize[A](ref: ReplyTo[A]): String = ref match {
    case r: ReplyRef[_] => s"${r.address}#${r.id}"
    case other          => throw new IllegalArgumentException(s"$other was not created by a cluster ask and cannot be sent to another node")
  }

  def resolve[A](serialized: String)(implicit codec: Codec[A, Array[Byte]]): ReplyTo[A] = serialized match {
    case Pattern(system, host, port, id) =>
      val address = Address(system, host, port.toInt)
      ReplyRef[A](address, id.toLong) { value =>
        codec.encode(value) match {
          case Right(bytes) => dispatcher.unsafeRunAndForget(remoting.reply(address, id.toLong, bytes))
          case Left(error)  => dispatcher.unsafeRunAndForget(log.warn(s"Cannot encode reply to $serialized: ${error.message}"))
        }
      }
    case _ => throw new IllegalArgumentException(s"'$serialized' is not a serialized ref")
  }
}

object IORefResolver {
  private val Pattern = """xplat://([^@]+)@(.+):(\d+)#(\d+)""".r
}

/** Implements `ask` for refs that may reach other nodes: the reply ref points back to this node. */
final class Asks[F[_]](remoting: Remoting[F], resolver: RefResolver[F])(implicit A: Async[F]) {

  def ask[T, B](send: T => F[Unit], f: ReplyTo[B] => T, timeout: FiniteDuration)(implicit codec: Codec[B, Array[Byte]]): F[B] =
    remoting
      .expect(timeout) { id =>
        A.delay(f(resolver.resolve[B](s"${remoting.self}#$id"))).flatMap(send)
      }
      .adaptError { case _: TimeoutException => new TimeoutException("Ask timeout") }
      .flatMap(bytes => A.fromEither(codec.decode(bytes).leftMap(e => XError(e.message))))
}

/** Reference to an endpoint at a fixed address. Two refs are equal when they point to the same endpoint. */
final case class EndpointRef[F[_], T](address: Address, endpoint: String)(remoting: Remoting[F], asks: Asks[F], codec: Codec[T, Array[Byte]])(implicit A: Async[F])
    extends ClusterRef[F, T] {

  def path: String = s"$address$endpoint"

  def send(msg: T): F[Unit] =
    A.fromEither(codec.encode(msg).leftMap(e => XError(e.message))).flatMap(remoting.send(address, endpoint, _))

  def ask[B](f: ReplyTo[B] => T, timeout: FiniteDuration)(implicit replyCodec: Codec[B, Array[Byte]]): F[B] =
    asks.ask[T, B](send, f, timeout)
}
