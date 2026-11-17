package xplat.flow.queue

import cats.effect.kernel.{Async, Resource}
import xplat.flow.nodes.ConnectionKind
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `queue`: `queue.publish`, the kinds of connection of the brokers and the source of the `queue` triggers on
  * them (a consumer per trigger, see [[QueueTriggers]]). Its publishers are a service of its own, on the clients of the server.
  */
final class QueuePlugin extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(
      FlowPlugin.Installed(
        List(new PublishNode[F](context.connections, PublisherPool.on[F](context.clients, PublisherFactory.connectors[F]), Some(context.traces))),
        triggers = List(QueueTriggers.source[F](ConsumerFactory.connectors[F], QueuePlugin.brokers))
      )
    )

  override def connectionKinds: List[ConnectionKind] = QueueNodes.connectionKinds
}

object QueuePlugin {
  val Name: String = "queue"

  private val brokers: Set[String] = QueueNodes.connectionKinds.map(_.kind).toSet
}
