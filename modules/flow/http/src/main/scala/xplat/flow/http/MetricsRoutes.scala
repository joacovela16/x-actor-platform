package xplat.flow.http

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.{EntityEncoder, HttpRoutes, MediaType, Response, Status}
import xplat.flow.engine.FlowMetrics
import xplat.flow.nodes.ClientPool

/** `/metrics` (Prometheus text) and `/api/health` (how each flow and each trigger source is going, for the UI). */
object MetricsRoutes {

  /** What is measured: the instances, the trigger sources and, if there is one, the pool of clients. */
  final case class Sources[F[_]](metrics: FlowMetrics[F], triggers: TriggerSources.View[F], clients: Option[ClientPool[F]] = None)

  def apply[F[_]](sources: Sources[F])(implicit F: Async[F]): HttpRoutes[F] = {
    val dsl = Http4sDsl[F]
    import dsl._

    def gauges: F[List[FlowMetrics.Sample]] =
      for {
        triggers <- sources.triggers.states
        clients <- sources.clients.fold(F.pure(List.empty[ClientPool.Client]))(_.clients)
      } yield {
        val states = List("running", "waiting", "unserved")
        triggers.flatMap(t =>
          states.map(s =>
            FlowMetrics.Sample("flow_trigger_source", "Triggers served by plugins, 1 in the state they are in.", List("trigger" -> t.triggerId, "state" -> s), if (t.state == s) 1 else 0)
          )
        ) ++
          clients.groupBy(_.connection).toList.sortBy(_._1).flatMap { case (connection, cs) =>
            List(
              FlowMetrics.Sample("flow_client_pool", "Clients of connections, in use or idle.", List("connection" -> connection, "state" -> "in_use"), cs.count(_.inUse > 0).toDouble),
              FlowMetrics.Sample("flow_client_pool", "Clients of connections, in use or idle.", List("connection" -> connection, "state" -> "idle"), cs.count(_.inUse == 0).toDouble)
            )
          }
      }

    HttpRoutes.of[F] {
      case GET -> Root / "metrics" =>
        (gauges.flatMap(sources.metrics.render)).map { text =>
          Response[F](Status.Ok)
            .withEntity(text)(EntityEncoder.stringEncoder[F])
            .withContentType(`Content-Type`(new MediaType("text", "plain", extensions = Map("version" -> "0.0.4"))))
        }

      case GET -> Root / "api" / "health" =>
        (sources.metrics.health, sources.triggers.states).tupled.flatMap { case (flows, triggers) =>
          Ok(
            Json.obj(
              "flows" -> Json.fromValues(flows.map { f =>
                Json.obj(
                  "flowId" -> f.flowId.asJson,
                  "running" -> f.running.asJson,
                  "succeeded" -> f.succeeded.asJson,
                  "failed" -> f.failed.asJson,
                  "p50Ms" -> f.p50Ms.asJson,
                  "p95Ms" -> f.p95Ms.asJson,
                  "lastFailures" -> Json.fromValues(f.lastFailures.map(x =>
                    Json.obj("instanceId" -> x.instanceId.asJson, "at" -> x.at.asJson, "durationMs" -> x.durationMs.asJson, "error" -> x.error.asJson)
                  ))
                )
              }),
              "triggers" -> Json.fromValues(triggers.sortBy(_.triggerId).map(t =>
                Json.obj("triggerId" -> t.triggerId.asJson, "kind" -> t.kind.asJson, "connection" -> t.connection.asJson, "state" -> t.state.asJson)
              ))
            )
          )
        }
    }
  }
}
