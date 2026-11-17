package xplat.flow.stream

import cats.Applicative
import io.circe.Json
import xplat.flow.model.Port

/** An element on its way through a stream segment.
  *
  * @param at
  *   when it entered the segment, in milliseconds since the epoch, to measure how long it takes to cross it.
  * @param ack
  *   what to do once it has been dealt with by the end of the segment: acknowledging the message of a queue it came from. An
  *   operator that drops an element, or turns many into one, takes care of it.
  * @param port
  *   the output port of the node it leaves through. Almost every node has only `out`; one that routes elements (see [[BranchOp]])
  *   says which. It is set back to `out` when the element enters the next node.
  */
final case class Elem[F[_]](value: Json, at: Long, ack: F[Unit], port: String = Port.Out) {

  /** Another value, which is what is made of this element: it has its place and its acknowledgement. */
  def withValue(v: Json): Elem[F] = copy(value = v)
}

object Elem {
  def of[F[_]](value: Json, at: Long)(implicit F: Applicative[F]): Elem[F] = Elem(value, at, F.unit)
}
