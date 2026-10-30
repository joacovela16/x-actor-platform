package xplat.core.cluster.api

import xplat.core.api.Codec

/** Name under which actors register in the [[xplat.core.cluster.Receptionist]]. The codec encodes the messages sent to
  * those actors from other nodes.
  */
final case class ServiceKey[T](id: String)(implicit val codec: Codec[T, Array[Byte]])

/** Actors registered under a key, on all reachable nodes. */
final case class Listing[F[_], T](key: ServiceKey[T], services: Set[ClusterRef[F, T]]) {
  def isEmpty: Boolean = services.isEmpty
}
