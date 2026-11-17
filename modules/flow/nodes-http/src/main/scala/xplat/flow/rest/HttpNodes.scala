package xplat.flow.rest

import cats.effect.Async
import xplat.flow.model.NodeCatalog
import xplat.flow.nodes.{Connections, NodeType, Traces}

/** The node that calls HTTP services (`http.request`), to give to the engine as `extraNodes`. It uses the `http` connections. */
object HttpNodes {

  val catalog: NodeCatalog = new NodeCatalog(List(HttpRequestNode.descriptor))

  def nodes[F[_]: Async](connections: Connections[F], traces: Option[Traces[F]] = None): List[NodeType[F]] = List(new HttpRequestNode[F](connections, traces))
}
