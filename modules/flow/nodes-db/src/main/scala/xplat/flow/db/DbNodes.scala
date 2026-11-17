package xplat.flow.db

import xplat.flow.nodes.ConnectionKind
import cats.effect.{Async, Resource}
import xplat.flow.model.NodeCatalog
import xplat.flow.nodes.{Connections, NodeType}
import xplat.flow.plugin.FlowPlugin
import xplat.flow.stream.{StreamNodeType, StreamPlugin}

/** The nodes that use databases, to give to the engine as `extraNodes`.
  *
  * {{{
  * DbNodes.resource[F](connections).use(nodes => ActorFlowEngine.make[F](system, journal, extraNodes = nodes))
  * }}}
  *
  * `connections` is how nodes find the named connections. The pools they open live as long as the resource, and are closed with it.
  * MongoDB has its own nodes, in `xplat.flow.mongo.MongoNodes`.
  */
object DbNodes {

  /** The kinds of connection that these nodes need, to pass to the `ConnectionStore`. */
  val connectionKinds: List[ConnectionKind] = DbKinds.all

  /** What the nodes are, for validation and the editor, without anything to run. */
  val catalog: NodeCatalog = new NodeCatalog(List(QueryNode.descriptor, ExecuteNode.descriptor, InsertNode.descriptor, UpdateNode.descriptor, DeleteNode.descriptor))

  def nodes[F[_]](connections: Connections[F], pool: DbPool[F])(implicit A: Async[F]): List[NodeType[F]] =
    List(new QueryNode[F](connections, pool), new ExecuteNode[F](connections, pool), new InsertNode[F](connections, pool), new UpdateNode[F](connections, pool), new DeleteNode[F](connections, pool))

  /** The stream source of these nodes (DbSource), for the plugin `stream`, on the clients of the server. */
  val streamExtra: StreamPlugin.Extra = new StreamPlugin.Extra {
    def apply[F[_]: Async](context: FlowPlugin.Context[F]): List[StreamNodeType[F]] = List(new DbSource[F](context.connections, DbPool.on[F](context.clients)))
  }

  def resource[F[_]](connections: Connections[F])(implicit A: Async[F]): Resource[F, List[NodeType[F]]] =
    DbPool.make[F].map(pool => nodes[F](connections, pool))
}
