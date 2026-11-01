package xplat.core.io.coordination

import cats.effect.kernel.{Async, Deferred, Ref, Resource}
import cats.effect.std.{Queue, Semaphore, Supervisor}
import cats.effect.syntax.all._
import cats.implicits._
import com.comcast.ip4s.{Host, Port, SocketAddress}
import fs2.io.net.{Network, Socket}
import xplat.core.api.net.TransportSecurity
import xplat.core.io.net.{BinReader, BinWriter, Frames, SecureChannel}

import scala.concurrent.duration._

/** Wire protocol between [[LeaseServer]] and [[RemoteLeaseStore]]. */
private object LeaseProtocol {
  val TryAcquire = 1
  val Renew = 2
  val Release = 3
  val Holder = 4

  final case class Request(id: Long, op: Int, name: String, owner: String, timeToLiveMillis: Long)

  def encode(r: Request): Array[Byte] =
    BinWriter().long(r.id).byte(r.op).string(r.name).string(r.owner).long(r.timeToLiveMillis).result()

  def decodeRequest(bytes: Array[Byte]): Request = {
    val in = BinReader(bytes)
    Request(in.long(), in.byte(), in.string(), in.string(), in.long())
  }

  /** A response carries the request id plus either a boolean or an optional holder. */
  final case class Response(id: Long, ok: Boolean, holder: Option[String])

  def encode(r: Response): Array[Byte] =
    BinWriter().long(r.id).bool(r.ok).option(r.holder)(_.string(_)).result()

  def decodeResponse(bytes: Array[Byte]): Response = {
    val in = BinReader(bytes)
    Response(in.long(), in.bool(), in.option(_.string()))
  }
}

/** Serves a [[LeaseStore]] over TCP, so nodes in different processes can share leases. Run it outside the cluster it
  * coordinates: if it ran inside, a split could leave it on one side only.
  *
  * With [[TransportSecurity]] the connections use mutual TLS and/or must prove they know the shared secret before any
  * request is served (see [[SecureChannel]]). Clients need the same settings.
  */
object LeaseServer {
  import LeaseProtocol._

  /** Starts the server. Port 0 picks a free port; the bound address is returned. */
  def bind[F[_]](host: Host, port: Port, store: LeaseStore[F], security: TransportSecurity = TransportSecurity())(implicit
      A: Async[F]
  ): Resource[F, SocketAddress[Host]] = {
    val network = Network.forAsync[F]
    for {
      channel    <- Resource.eval(SecureChannel[F](network, security))
      supervisor <- Supervisor[F](await = false)
      server     <- network.serverResource(Some(host), Some(port))
      (address, clients) = server
      // Each client is served inside the stream: a socket is closed as soon as its stream element goes out of scope.
      _ <- Resource.eval(
        supervisor.supervise(
          clients
            .map(socket =>
              fs2.Stream.eval(
                channel
                  .server(socket)
                  .use(secured => channel.accept(secured)(_ => A.pure(Array.emptyByteArray)).flatMap(_.traverse_(_ => serve(secured, store))))
                  .attempt
                  .void
              )
            )
            .parJoinUnbounded
            .compile
            .drain
        )
      )
    } yield SocketAddress[Host](address.host, address.port)
  }

  private def serve[F[_]](socket: Socket[F], store: LeaseStore[F])(implicit A: Async[F]): F[Unit] =
    Frames
      .read(socket)
      .evalMap { bytes =>
        val request = decodeRequest(bytes)
        val ttl = request.timeToLiveMillis.millis
        val result: F[Response] = request.op match {
          case TryAcquire => store.tryAcquire(request.name, request.owner, ttl).map(Response(request.id, _, None))
          case Renew      => store.renew(request.name, request.owner, ttl).map(Response(request.id, _, None))
          case Release    => store.release(request.name, request.owner).map(Response(request.id, _, None))
          case Holder     => store.holder(request.name).map(h => Response(request.id, h.isDefined, h))
          case other      => A.raiseError(new IllegalArgumentException(s"unknown lease operation $other"))
        }
        result.flatMap(response => Frames.write(socket, encode(response)))
      }
      .compile
      .drain
      .attempt
      .void
}

/** [[LeaseStore]] that talks to a [[LeaseServer]]. It connects lazily and reconnects after a failure; an operation that
  * fails because of the connection raises an error, which leases treat as a failed renewal.
  */
final class RemoteLeaseStore[F[_]] private (
    server: SocketAddress[Host],
    channel: SecureChannel[F],
    supervisor: Supervisor[F],
    connection: Ref[F, Option[RemoteLeaseStore.Connection[F]]],
    connecting: Semaphore[F],
    nextId: Ref[F, Long]
)(implicit A: Async[F])
    extends LeaseStore[F] {
  import LeaseProtocol._
  import RemoteLeaseStore.Connection

  private def connect: F[Connection[F]] = connecting.permit.use { _ =>
    connection.get.flatMap {
      case Some(open) => A.pure(open)
      case None =>
        for {
          closed  <- Deferred[F, Unit]
          pending <- Ref.of[F, Map[Long, Deferred[F, Either[Throwable, Response]]]](Map.empty)
          outbox  <- Queue.unbounded[F, Array[Byte]]
          ready   <- Deferred[F, Either[Throwable, Unit]]
          conn = Connection(outbox, pending)
          run = Network
            .forAsync[F]
            .client(server)
            .flatMap(channel.client)
            .use { socket =>
              channel.connect(socket, Array.emptyByteArray) >> ready.complete(Right(())) >>
                A.race(
                  fs2.Stream.fromQueueUnterminated(outbox).evalMap(Frames.write(socket, _)).compile.drain,
                  Frames.read(socket).evalMap(bytes => complete(pending, decodeResponse(bytes))).compile.drain
                ).void
            }
            .guarantee(
              ready.complete(Left(new java.io.IOException(s"cannot connect to lease server $server or it rejected this client"))).void >>
                connection.set(None) >> failAll(pending) >> closed.complete(()).void
            )
          _ <- connection.set(Some(conn))
          _ <- supervisor.supervise(run.attempt)
          _ <- ready.get.rethrow
        } yield conn
    }
  }

  private def complete(pending: Ref[F, Map[Long, Deferred[F, Either[Throwable, Response]]]], response: Response): F[Unit] =
    pending.modify(m => (m - response.id, m.get(response.id))).flatMap(_.traverse_(_.complete(Right(response))))

  private def failAll(pending: Ref[F, Map[Long, Deferred[F, Either[Throwable, Response]]]]): F[Unit] =
    pending.getAndSet(Map.empty).flatMap(_.values.toList.traverse_(_.complete(Left(new java.io.IOException("lease server connection closed")))))

  private def request(op: Int, name: String, owner: String, timeToLive: FiniteDuration): F[Response] =
    for {
      conn   <- connect
      id     <- nextId.getAndUpdate(_ + 1)
      answer <- Deferred[F, Either[Throwable, Response]]
      _      <- conn.pending.update(_ + (id -> answer))
      _      <- conn.outbox.offer(encode(Request(id, op, name, owner, timeToLive.toMillis)))
      result <- answer.get.rethrow.onCancel(conn.pending.update(_ - id))
    } yield result

  def tryAcquire(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] = request(TryAcquire, name, owner, timeToLive).map(_.ok)
  def renew(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] = request(Renew, name, owner, timeToLive).map(_.ok)
  def release(name: String, owner: String): F[Boolean] = request(Release, name, owner, Duration.Zero).map(_.ok)
  def holder(name: String): F[Option[String]] = request(Holder, name, "", Duration.Zero).map(_.holder)
}

object RemoteLeaseStore {
  private final case class Connection[F[_]](outbox: Queue[F, Array[Byte]], pending: Ref[F, Map[Long, Deferred[F, Either[Throwable, LeaseProtocol.Response]]]])

  /** Client of the [[LeaseServer]] at `server`, with the same security settings as the server. */
  def apply[F[_]](server: SocketAddress[Host], security: TransportSecurity = TransportSecurity())(implicit A: Async[F]): Resource[F, LeaseStore[F]] =
    for {
      channel    <- Resource.eval(SecureChannel[F](Network.forAsync[F], security))
      supervisor <- Supervisor[F](await = false)
      connection <- Resource.eval(Ref.of[F, Option[Connection[F]]](None))
      connecting <- Resource.eval(Semaphore[F](1))
      nextId     <- Resource.eval(Ref.of[F, Long](0L))
    } yield new RemoteLeaseStore[F](server, channel, supervisor, connection, connecting, nextId)
}
