package xplat.flow.stream

import xplat.flow.model.NodeDescriptor

/** A kind of node that takes part in a stream. It does not run as a node of an instance: the segment that contains it compiles it
  * into the `fs2.Stream` that the whole segment is. Its [[descriptor]] has stream ports (see [[xplat.flow.model.Port]]).
  *
  * It is one of [[StreamSource]], [[StreamOperator]] or [[StreamSink]].
  */
trait StreamNodeType[F[_]] {
  def descriptor: NodeDescriptor
}
