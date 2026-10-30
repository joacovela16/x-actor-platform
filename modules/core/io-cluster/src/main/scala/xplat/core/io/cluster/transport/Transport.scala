package xplat.core.io.cluster.transport

import cats.effect.kernel.Resource
import xplat.core.cluster.api.Address

/** Moves bytes between cluster nodes. Delivery is best effort: messages to a node that cannot be reached are dropped.
  * Messages between two nodes arrive in the order they were sent.
  */
trait Transport[F[_]] {

  /** Address other nodes use to reach this one. */
  def address: Address

  def send(to: Address, payload: Array[Byte]): F[Unit]
}

trait TransportFactory[F[_]] {

  /** Starts listening. Port 0 picks a free port, so read the actual one from [[Transport.address]]. */
  def bind(system: String, host: String, port: Int, onReceive: Array[Byte] => F[Unit]): Resource[F, Transport[F]]
}
