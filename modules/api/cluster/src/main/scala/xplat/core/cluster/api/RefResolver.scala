package xplat.core.cluster.api

import xplat.core.api.{Codec, ReplyTo}

/** Lets reply refs travel inside messages sent to other nodes.
  *
  * A [[Ref]] only lives in the node that created it. To `ask` an actor on another node, the message codec writes the ref
  * with [[serialize]], and the receiving node turns it back into a working ref with [[resolve]]. Replies sent through the
  * resolved ref are encoded with the given codec and routed back to the asking node.
  *
  * Both operations are pure so they can be used inside a [[Codec]]. Build the message codecs of a node with that node's
  * resolver (`cluster.refResolver`).
  */
trait RefResolver[F[_]] {

  /** Fails with `IllegalArgumentException` for refs that were not created by a cluster ask or by [[resolve]]. */
  def serialize[A](ref: ReplyTo[A]): String

  /** Fails with `IllegalArgumentException` if `serialized` was not produced by [[serialize]]. */
  def resolve[A](serialized: String)(implicit codec: Codec[A, Array[Byte]]): ReplyTo[A]
}
