package xplat.flow.ai

import cats.effect.kernel.{Async, Resource}
import xplat.flow.nodes.ConnectionKind
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `ai`: `ai.agent` and the kinds of connection of the AI services. Its HTTP client is a service of its own,
  * opened when it is installed and closed when the server stops.
  *
  * @param cassettes
  *   whether the answers of the services are recorded to files or played back from them.
  */
final class AiPlugin(cassettes: Cassettes.Mode = Cassettes.Mode.Off) extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    AiNodes.resource[F](context.connections, cassettes).map(FlowPlugin.Installed(_))

  override def connectionKinds: List[ConnectionKind] = AiNodes.connectionKinds
}

object AiPlugin {
  val Name: String = "ai"
}
