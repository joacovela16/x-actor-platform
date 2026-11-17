package xplat.flow.http

import cats.effect.Async
import cats.effect.std.Console
import cats.syntax.semigroupk._
import org.http4s.HttpRoutes
import xplat.flow.engine.{Deployments, FlowEngine}

/** All the routes of the flow module, over any `F`: the API under `/api`, the hooks of the triggers under `/hooks`, and the UI.
  * Whoever starts the server (see `flow-server`) only has to give them an HTTP server.
  */
object FlowHttp {

  def routes[F[_]: Async: Console](
      engine: FlowEngine[F],
      triggers: TriggerRegistry[F],
      connections: ConnectionStore[F],
      secrets: SecretStore[F],
      deployments: Deployments[F],
      drafts: FlowDrafts[F],
      clients: Option[xplat.flow.nodes.ClientPool[F]] = None,
      plugins: io.circe.Json = io.circe.Json.arr(),
      staticRoot: String = "/public",
      entities: Option[EntityStore[F]] = None,
      metrics: Option[MetricsRoutes.Sources[F]] = None,
      tracing: Option[xplat.flow.engine.FlowTracing[F]] = None
  ): HttpRoutes[F] =
    ErrorHandler(
      FlowRoutes(engine, deployments, drafts) <+> AdminRoutes(engine, triggers, connections, clients, plugins) <+>
        entities.fold(HttpRoutes.empty[F])(EntityRoutes(engine, _)) <+>
        BundleRoutes(new Bundle.Service[F](engine, triggers, connections, deployments, drafts, entities)) <+>
        metrics.fold(HttpRoutes.empty[F])(MetricsRoutes(_)) <+> HookRoutes(engine, triggers, secrets, tracing) <+> StaticRoutes(staticRoot)
    )
}
