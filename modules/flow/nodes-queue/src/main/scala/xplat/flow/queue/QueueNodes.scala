package xplat.flow.queue

import xplat.flow.nodes.ConnectionKind
import cats.effect.{Async, Resource}
import xplat.flow.model.NodeCatalog
import xplat.flow.nodes.{Connections, NodeType}

/** The nodes that talk to message brokers, to give to the engine as `extraNodes`.
  *
  * {{{
  * QueueNodes.resource[F](connections).use(nodes => ActorFlowEngine.make[F](system, journal, extraNodes = nodes))
  * }}}
  *
  * `connections` is how nodes find the named connections (the HTTP layer provides one from its `ConnectionStore`). The
  * publishers they open live as long as the resource.
  */
object QueueNodes {

  /** The kinds of connection that these nodes need, to pass to the `ConnectionStore`. */
  val connectionKinds: List[ConnectionKind] = QueueKinds.all

  /** What the nodes are, for validation and the editor, without anything to run. */
  val catalog: NodeCatalog = new NodeCatalog(List(PublishNode.descriptor))

  def resource[F[_]](connections: Connections[F], factory: PublisherFactory[F])(implicit A: Async[F]): Resource[F, List[NodeType[F]]] =
    PublisherPool.make[F](factory).map(pool => List(new PublishNode[F](connections, pool)))

  def resource[F[_]](connections: Connections[F])(implicit A: Async[F]): Resource[F, List[NodeType[F]]] =
    resource[F](connections, PublisherFactory.connectors[F])
}
