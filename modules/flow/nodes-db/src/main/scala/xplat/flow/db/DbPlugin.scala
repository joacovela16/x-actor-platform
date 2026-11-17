package xplat.flow.db

import cats.effect.kernel.{Async, Resource}
import xplat.flow.nodes.ConnectionKind
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `db`: the nodes of relational databases and their kinds of connection. Its pool of JDBC connections is a
  * service of its own, on the clients of the server.
  */
final class DbPlugin extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(FlowPlugin.Installed(DbNodes.nodes[F](context.connections, DbPool.on[F](context.clients))))

  override def connectionKinds: List[ConnectionKind] = DbNodes.connectionKinds
}

object DbPlugin {
  val Name: String = "db"
}
