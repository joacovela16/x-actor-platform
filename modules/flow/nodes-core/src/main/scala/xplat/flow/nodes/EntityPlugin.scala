package xplat.flow.nodes

import cats.effect.kernel.{Async, Resource}
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `entity`: the nodes that build, validate and move values of the entities of the business model, which
  * they read from the context each time they run.
  */
final class EntityPlugin extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(FlowPlugin.Installed(EntityNodes.nodes[F](context.entities)))
}

object EntityPlugin {
  val Name: String = "entity"
}
