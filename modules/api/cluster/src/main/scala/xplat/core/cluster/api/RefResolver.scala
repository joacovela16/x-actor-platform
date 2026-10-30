package xplat.core.cluster.api

import xplat.core.api.{Codec, Ref}

/** Lets reply refs travel inside messages sent to other nodes.
  *
  * A [[Ref]] only lives in the node that created it. To `ask` an actor on another node, the message codec writes the ref
  * with [[serialize]], and the receiving node turns it back into a working ref with [[resolve]]. Replies sent through the
  * resolved ref are encoded with the given codec and routed back to the asking node.
  */
trait RefResolver[F[_]] {
  def serialize[A](ref: Ref[A]): String
  def resolve[A](serialized: String)(implicit codec: Codec[A, Array[Byte]]): F[Ref[A]]
}
