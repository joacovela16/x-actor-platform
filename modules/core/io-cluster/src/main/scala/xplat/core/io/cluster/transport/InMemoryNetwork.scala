package xplat.core.io.cluster.transport

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Queue
import cats.implicits._
import xplat.core.io.net.SecureChannel
import xplat.core.api.net.TransportSecurity
import xplat.core.cluster.api.Address

/** Network of nodes running in the same process, for tests and local development. It can simulate network splits.
  *
  * Messages are not encrypted, since nothing leaves the process, but the security settings are checked as a TCP
  * handshake would: two nodes only talk if they share the secret and, with TLS, trust each other's certificate.
  * Messages between nodes that would be rejected are dropped.
  */
final class InMemoryNetwork[F[_]] private (
    nodes: Ref[F, Map[Address, (Queue[F, Array[Byte]], TransportSecurity)]],
    compatibility: Ref[F, Map[(TransportSecurity, TransportSecurity), Boolean]],
    blocked: Ref[F, Set[(Address, Address)]],
    nextPort: Ref[F, Int]
)(implicit A: Async[F])
    extends TransportFactory[F] {

  def bind(system: String, host: String, port: Int, security: TransportSecurity, onReceive: Array[Byte] => F[Unit]): Resource[F, Transport[F]] =
    for {
      actualPort <- Resource.eval(if (port == 0) nextPort.getAndUpdate(_ + 1) else A.pure(port))
      address = Address(system, host, actualPort)
      inbox <- Resource.eval(Queue.unbounded[F, Array[Byte]])
      _ <- Resource.make(
        nodes.modify { current =>
          if (current.contains(address)) (current, A.raiseError[Unit](new IllegalStateException(s"$address is already bound")))
          else (current.updated(address, (inbox, security)), A.unit)
        }.flatten
      )(_ => nodes.update(_ - address))
      // One consumer per node keeps messages in order and never blocks the sender.
      _ <- A.background(fs2.Stream.fromQueueUnterminated(inbox).evalMap(bytes => onReceive(bytes).attempt.void).compile.drain)
    } yield new Transport[F] {
      val address: Address = Address(system, host, actualPort)
      def send(to: Address, payload: Array[Byte]): F[Unit] =
        blocked.get.flatMap { b =>
          if (b((address, to))) A.unit
          else
            nodes.get.flatMap(_.get(to).traverse_ { case (inbox, theirs) =>
              compatible(security, theirs).ifM(inbox.offer(payload), A.unit)
            })
        }
    }

  /** Checking certificates loads the stores, so each pair of settings is checked once. */
  private def compatible(a: TransportSecurity, b: TransportSecurity): F[Boolean] =
    compatibility.get.map(_.get((a, b))).flatMap {
      case Some(known) => A.pure(known)
      case None        => A.blocking(SecureChannel.compatible(a, b)).flatTap(ok => compatibility.update(_.updated((a, b), ok)))
    }

  /** Cuts the network between the two groups: no message crosses in either direction until [[heal]]. */
  def partition(side: Set[Address], other: Set[Address]): F[Unit] =
    blocked.update(_ ++ (for (a <- side; b <- other; pair <- Seq((a, b), (b, a))) yield pair))

  /** Restores every connection. */
  def heal: F[Unit] = blocked.set(Set.empty)
}

object InMemoryNetwork {
  def apply[F[_]](implicit A: Async[F]): F[InMemoryNetwork[F]] =
    (
      Ref.of[F, Map[Address, (Queue[F, Array[Byte]], TransportSecurity)]](Map.empty),
      Ref.of[F, Map[(TransportSecurity, TransportSecurity), Boolean]](Map.empty),
      Ref.of[F, Set[(Address, Address)]](Set.empty),
      Ref.of[F, Int](30000)
    ).mapN(new InMemoryNetwork[F](_, _, _, _))
}
