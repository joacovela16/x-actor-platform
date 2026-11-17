package xplat.flow.script

import cats.effect.kernel.{Async, Resource}
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `script`: `script.js`, run in a GraalJS sandbox, and `script.scala`, compiled in the server and NOT
  * sandboxed (offered only when `scala`). The code of a script node is part of the flow; these are the kinds that run it.
  */
final class ScriptPlugin(scala: Boolean = true) extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(FlowPlugin.Installed(ScriptNodes.nodes[F]() ++ (if (scala) ScalaNode.nodes[F] else Nil)))
}

object ScriptPlugin {
  val Name: String = "script"
}
