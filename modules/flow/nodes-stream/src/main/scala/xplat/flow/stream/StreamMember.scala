package xplat.flow.stream

import cats.Applicative
import xplat.flow.model.{NodeDescriptor, NodeInput, NodeOutput}
import xplat.flow.nodes.NodeType

/** The kind of node of a stream, as the engine sees it: it has a description, for the catalog and the editor, and does not run by
  * itself. It runs inside the segment that contains it (see [[SegmentNode]]), which the engine makes out of the flow.
  */
final class StreamMember[F[_]](node: StreamNodeType[F])(implicit F: Applicative[F]) extends NodeType[F] {

  val descriptor: NodeDescriptor = node.descriptor

  def run(input: NodeInput): F[NodeOutput] = F.pure(NodeOutput.failed(s"'${descriptor.kind}' only runs inside a stream"))
}
