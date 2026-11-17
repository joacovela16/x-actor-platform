package xplat.flow.model

/** The kinds of node available, by name. */
final class NodeCatalog(descriptors: Iterable[NodeDescriptor]) {

  private val byKind: Map[String, NodeDescriptor] = {
    val kinds = descriptors.map(_.kind).toList
    require(kinds.distinct.size == kinds.size, s"kinds registered twice: ${kinds.diff(kinds.distinct).distinct.mkString(", ")}")
    descriptors.map(d => d.kind -> d).toMap
  }

  def get(kind: String): Option[NodeDescriptor] = byKind.get(kind)

  def all: List[NodeDescriptor] = byKind.values.toList.sortBy(_.kind)

  def ++(other: NodeCatalog): NodeCatalog = new NodeCatalog(all ++ other.all)
}

object NodeCatalog {
  val empty: NodeCatalog = new NodeCatalog(Nil)
}
