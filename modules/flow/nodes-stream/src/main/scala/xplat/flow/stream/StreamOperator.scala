package xplat.flow.stream

import fs2.Stream

/** A node in the middle of a stream: it turns the streams that enter it into the one that leaves it.
  *
  * Operators take care of the acknowledgements of the elements they receive: one that passes an element on keeps its `ack`, one
  * that drops it calls [[StreamContext.drop]] or [[StreamContext.fail]], and one that makes one element out of many either combines
  * their acknowledgements or acknowledges them when it takes them in (when the state it keeps is only in memory anyway).
  */
trait StreamOperator[F[_]] extends StreamNodeType[F] {

  /** @param inputs
    *   the stream that enters each input of the node, by name of the input (`in`, or `left` and `right`), already merged if several
    *   edges enter the same input. An input that nothing is connected to is not in the map.
    */
  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]]
}
