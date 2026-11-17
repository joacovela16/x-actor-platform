package xplat.flow.mongo

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import io.circe.Json
import xplat.core.connectors.Sink
import xplat.flow.http.{TriggerDef, TriggerSources}
import xplat.flow.nodes.{ConnectionKind, Connections, TriggerSource}
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `mongo`: the nodes of MongoDB, its kind of connection and the source of the `queue` triggers on a `mongodb`
  * connection, whose queue is a collection and whose messages are its changes ([[ChangeStreams]]). Its pool of clients is a service
  * of its own, on the clients of the server.
  */
final class MongoPlugin extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(FlowPlugin.Installed(MongoNodes.nodes[F](context.connections, MongoPool.on[F](context.clients)), triggers = List(MongoPlugin.changes[F])))

  override def connectionKinds: List[ConnectionKind] = MongoNodes.connectionKinds
}

object MongoPlugin {
  val Name: String = "mongo"

  /** `queue` triggers on `mongodb` connections: each change of the collection starts an instance. */
  def changes[F[_]](implicit A: Async[F]): TriggerSource[F] = new TriggerSource[F] {
    val kind: String = TriggerDef.Kind.Queue.name
    def accepts(connectionKind: String): Boolean = connectionKind == MongoKinds.Mongo
    def run(trigger: Json, connection: Connections.Resolved, start: TriggerSource.Start[F]): F[Unit] =
      A.fromEither(trigger.as[TriggerDef]).flatMap { t =>
        val sink = Sink[F, Array[Byte]] { m =>
          start(
            TriggerSources.messageRequest(m.source, m.key, m.position, m.headers, m.value),
            TriggerSources.messageKey(m.source, m.position, m.headers),
            Some(m.position)
          ).void
        }
        ChangeStreams.consume[F](connection, t.queue.getOrElse(""), t.consumerGroup, sink).use(_.stopped)
      }
  }
}
