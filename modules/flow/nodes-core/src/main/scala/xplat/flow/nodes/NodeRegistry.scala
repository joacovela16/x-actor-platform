package xplat.flow.nodes

import xplat.flow.model.NodeCatalog

/** The kinds of node that can run. Core nodes and those of other modules are registered in the same way. */
final class NodeRegistry[F[_]](val types: List[NodeType[F]]) {

  private val byKind: Map[String, NodeType[F]] = {
    val kinds = types.map(_.descriptor.kind)
    require(kinds.distinct.size == kinds.size, s"kinds registered twice: ${kinds.diff(kinds.distinct).distinct.mkString(", ")}")
    types.map(t => t.descriptor.kind -> t).toMap
  }

  def get(kind: String): Option[NodeType[F]] = byKind.get(kind)

  /** The descriptions of the kinds, which is what validation and the editor need. */
  val catalog: NodeCatalog = new NodeCatalog(types.map(_.descriptor))

  def ++(other: NodeRegistry[F]): NodeRegistry[F] = new NodeRegistry(types ++ other.types)
}

object NodeRegistry {
  def apply[F[_]](types: NodeType[F]*): NodeRegistry[F] = new NodeRegistry(types.toList)
}
