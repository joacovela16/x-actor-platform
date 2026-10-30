package xplat.core.io.cluster.transport

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Queue
import cats.implicits._
import xplat.core.cluster.api.{Address, TransportSecurity}

/** Network of nodes running in the same process, for tests and local development. It can simulate network splits.
  * Messages are not encrypted and the security settings are ignored, since nothing leaves the process.
  */
final class InMemoryNetwork[F[_]] private (
    nodes: Ref[F, Map[Address, Queue[F, Array[Byte]]]],
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
          else (current.updated(address, inbox), A.unit)
        }.flatten
      )(_ => nodes.update(_ - address))
      // One consumer per node keeps messages in order and never blocks the sender.
      _ <- A.background(fs2.Stream.fromQueueUnterminated(inbox).evalMap(bytes => onReceive(bytes).attempt.void).compile.drain)
    } yield new Transport[F] {
      val address: Address = Address(system, host, actualPort)
      def send(to: Address, payload: Array[Byte]): F[Unit] =
        blocked.get.flatMap { b =>
          if (b((address, to))) A.unit
          else nodes.get.flatMap(_.get(to).traverse_(_.offer(payload)))
        }
    }

  /** Cuts the network between the two groups: no message crosses in either direction until [[heal]]. */
  def partition(side: Set[Address], other: Set[Address]): F[Unit] =
    blocked.update(_ ++ (for (a <- side; b <- other; pair <- Seq((a, b), (b, a))) yield pair))

  /** Restores every connection. */
  def heal: F[Unit] = blocked.set(Set.empty)
}

object InMemoryNetwork {
  def apply[F[_]](implicit A: Async[F]): F[InMemoryNetwork[F]] =
    (Ref.of[F, Map[Address, Queue[F, Array[Byte]]]](Map.empty), Ref.of[F, Set[(Address, Address)]](Set.empty), Ref.of[F, Int](30000))
      .mapN(new InMemoryNetwork[F](_, _, _))
}
