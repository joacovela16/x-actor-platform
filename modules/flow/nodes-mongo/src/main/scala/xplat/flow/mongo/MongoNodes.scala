package xplat.flow.mongo

import xplat.flow.nodes.ConnectionKind
import cats.effect.{Async, Resource}
import xplat.flow.model.NodeCatalog
import xplat.flow.nodes.{Connections, NodeType}

/** The nodes that use MongoDB, to give to the engine as `extraNodes`:
  *
  * {{{
  * MongoNodes.resource[F](connections).use(nodes => ActorFlowEngine.make[F](system, journal, extraNodes = nodes))
  * }}}
  *
  * The clients they open live as long as the resource, and are closed with it.
  */
object MongoNodes {

  /** The kinds of connection that these nodes need, to pass to the `ConnectionStore`. */
  val connectionKinds: List[ConnectionKind] = MongoKinds.all

  val catalog: NodeCatalog = new NodeCatalog(List(FindNode.descriptor, AggregateNode.descriptor, InsertNode.descriptor, UpdateNode.descriptor, DeleteNode.descriptor))

  def nodes[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit A: Async[F]): List[NodeType[F]] =
    List(new FindNode[F](connections, pool), new AggregateNode[F](connections, pool), new InsertNode[F](connections, pool), new UpdateNode[F](connections, pool), new DeleteNode[F](connections, pool))

  def resource[F[_]](connections: Connections[F])(implicit A: Async[F]): Resource[F, List[NodeType[F]]] =
    MongoPool.make[F].map(pool => nodes[F](connections, pool))
}
