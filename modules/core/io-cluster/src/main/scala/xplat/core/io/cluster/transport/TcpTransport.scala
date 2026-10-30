package xplat.core.io.cluster.transport

import cats.effect.kernel.{Async, Clock, Ref, Resource}
import cats.effect.std.{Queue, Supervisor}
import cats.effect.syntax.all._
import cats.implicits._
import com.comcast.ip4s.{Host, Port, SocketAddress}
import fs2.io.net.Network
import xplat.core.cluster.api.Address
import xplat.core.io.net.Frames

import scala.concurrent.duration._

/** Transport over TCP. Each node keeps one outgoing connection per destination, opened on the first message. When a
  * connection fails, messages are dropped for [[TcpTransport.ReconnectBackoff]] and then it is opened again.
  */
final class TcpTransport[F[_]](outboxSize: Int)(implicit A: Async[F]) extends TransportFactory[F] {
  import TcpTransport._

  def bind(system: String, host: String, port: Int, onReceive: Array[Byte] => F[Unit]): Resource[F, Transport[F]] = {
    val network = Network.forAsync[F]
    for {
      bindHost <- Resource.eval(A.fromOption(Host.fromString(host), new IllegalArgumentException(s"invalid host $host")))
      bindPort <- Resource.eval(A.fromOption(Port.fromInt(port), new IllegalArgumentException(s"invalid port $port")))
      supervisor <- Supervisor[F](await = false)
      server     <- network.serverResource(Some(bindHost), Some(bindPort))
      (bound, clients) = server
      // Each connection is read inside the stream: a socket is closed when its stream element goes out of scope.
      _ <- Resource.eval(
        supervisor.supervise(
          clients
            .map(socket => Frames.read(socket).evalMap(bytes => onReceive(bytes).attempt.void).handleErrorWith(_ => fs2.Stream.empty))
            .parJoinUnbounded
            .compile
            .drain
        )
      )
      links <- Resource.eval(Ref.of[F, Map[Address, Link[F]]](Map.empty))
    } yield new TcpConnections[F](Address(system, host, bound.port.value), links, supervisor, outboxSize, network)
  }
}

object TcpTransport {

  /** Time without sending to a node after its connection failed, so a dead node is not dialled on every message. */
  val ReconnectBackoff: FiniteDuration = 1.second

  def apply[F[_]: Async]: TransportFactory[F] = new TcpTransport[F](outboxSize = 10000)

  private[transport] sealed trait Link[F[_]]
  private[transport] final case class Open[F[_]](outbox: Queue[F, Array[Byte]]) extends Link[F]
  private[transport] final case class Failed[F[_]](until: FiniteDuration) extends Link[F]
}

private final class TcpConnections[F[_]](
    val address: Address,
    links: Ref[F, Map[Address, TcpTransport.Link[F]]],
    supervisor: Supervisor[F],
    outboxSize: Int,
    network: Network[F]
)(implicit A: Async[F])
    extends Transport[F] {
  import TcpTransport._

  def send(to: Address, payload: Array[Byte]): F[Unit] =
    outbox(to).flatMap(_.traverse_(_.tryOffer(payload).void))

  private def outbox(to: Address): F[Option[Queue[F, Array[Byte]]]] =
    Clock[F].monotonic.flatMap { now =>
      Queue.bounded[F, Array[Byte]](outboxSize).flatMap { fresh =>
        links.modify { current =>
          current.get(to) match {
            case Some(Open(queue))                => (current, (Some(queue), false))
            case Some(Failed(until)) if until > now => (current, (None, false))
            case _                                => (current.updated(to, Open(fresh)), (Some(fresh), true))
          }
        }.flatMap { case (queue, created) =>
          A.whenA(created)(supervisor.supervise(connect(to, fresh)).void).as(queue)
        }
      }
    }

  private def connect(to: Address, queue: Queue[F, Array[Byte]]): F[Unit] = {
    val target = for {
      host <- Host.fromString(to.host)
      port <- Port.fromInt(to.port)
    } yield SocketAddress(host, port)

    val run = A.fromOption(target, new IllegalArgumentException(s"invalid address $to")).flatMap { socketAddress =>
      network.client(socketAddress).use { socket =>
        fs2.Stream.fromQueueUnterminated(queue).evalMap(Frames.write(socket, _)).compile.drain
      }
    }

    run.attempt.void.guarantee(Clock[F].monotonic.flatMap(now => links.update(_.updated(to, Failed(now + ReconnectBackoff)))))
  }
}
