package xplat.flow.engine

import cats.effect.kernel.{Async, Resource}
import xplat.flow.nodes.CoreNodes
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `core`: `core.if`, `core.subflow`, `core.respond` and `core.cron`. The subflow node starts flows through the
  * runner of the context; the cron node counts time with [[CronSchedule]].
  */
final class CorePlugin extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(FlowPlugin.Installed(CoreNodes.registry[F](context.runner, CronSchedule.nextFire).types))
}

object CorePlugin {
  val Name: String = "core"
}
