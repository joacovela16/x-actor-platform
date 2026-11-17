package xplat.flow.rest

import cats.effect.kernel.{Async, Resource}
import xplat.flow.plugin.FlowPlugin

/** The built-in plugin `http`: `http.request`. Its connections are of the kind `http` of the server. */
final class HttpPlugin extends FlowPlugin {
  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(FlowPlugin.Installed(HttpNodes.nodes[F](context.connections, Some(context.traces))))
}

object HttpPlugin {
  val Name: String = "http"
}
