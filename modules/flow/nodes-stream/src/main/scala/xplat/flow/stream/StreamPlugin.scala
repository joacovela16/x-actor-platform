package xplat.flow.stream

import cats.effect.kernel.{Async, Resource}
import xplat.flow.plugin.FlowPlugin
import xplat.flow.queue.{ConsumerFactory, PublisherFactory, PublisherPool}

/** The built-in plugin `stream`: `stream.segment` and the nodes that can be in a segment. It reads and publishes to the brokers
  * with publishers of its own (on the clients of the server), runs flows with the runner of the context and reports how segments
  * go to its metrics.
  *
  * @param extra
  *   kinds of stream node made elsewhere that segments can contain too (`stream.script`).
  */
final class StreamPlugin(extra: StreamPlugin.Extra = StreamPlugin.Extra.none, settings: SegmentNode.Settings = SegmentNode.Settings()) extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(
      FlowPlugin.Installed(
        StreamNodes.nodes[F](
          context.connections,
          ConsumerFactory.connectors[F],
          PublisherPool.on[F](context.clients, PublisherFactory.connectors[F]),
          context.metrics,
          context.runner,
          settings,
          extra[F],
          Some(context.traces)
        )
      )
    )
}

object StreamPlugin {
  val Name: String = "stream"

  /** Kinds of stream node for any effect. */
  trait Extra {
    def apply[F[_]: Async]: List[StreamNodeType[F]]
  }

  object Extra {
    val none: Extra = new Extra {
      def apply[F[_]: Async]: List[StreamNodeType[F]] = Nil
    }
  }
}
