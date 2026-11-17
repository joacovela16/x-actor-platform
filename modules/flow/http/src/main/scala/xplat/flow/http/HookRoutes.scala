package xplat.flow.http

import cats.effect.Async
import cats.effect.syntax.temporal._
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Header, HttpRoutes, Request, Response, Status}
import org.typelevel.ci.CIString
import xplat.flow.engine._
import xplat.flow.model.StatePath
import xplat.flow.nodes.{RespondNode, Traces}

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.concurrent.duration._

/** The `/hooks` routes: the requests that HTTP triggers serve. The trigger is found by method and path in the registry, which can change
  * while the server runs.
  *
  * A request is checked against the authentication of the trigger and starts an instance of its flow. An asynchronous trigger
  * answers `202 {instanceId}` at once. A synchronous one waits, up to its timeout, for a `core.respond` node to finish and
  * answers what it describes; if the flow ends without one it answers 200 (or 500 if it failed) with the id and status, and if
  * the time runs out it answers 202 like an asynchronous one.
  *
  * The header `Idempotency-Key` makes a request that is repeated run once: the instance gets an id made of the trigger and the
  * key, so the second request finds the same instance and gets the same answer.
  */
object HookRoutes {

  private val DefaultTimeout: FiniteDuration = 10.seconds

  private def instanceIdOf(trigger: TriggerDef, key: String): String = s"${trigger.id}-$key".replaceAll("[^A-Za-z0-9_.-]", "_")

  /** @param tracing
    *   when traces are on: a request with a `traceparent` header starts an instance that goes on that trace.
    */
  def apply[F[_]: Async](engine: FlowEngine[F], triggers: TriggerRegistry[F], secrets: SecretStore[F], tracing: Option[FlowTracing[F]] = None): HttpRoutes[F] = {
    val dsl = Http4sDsl[F]
    import dsl._

    def problem(status: Status, code: String, message: String): F[Response[F]] =
      Async[F].pure(Response[F](status).withEntity(ApiJson.problems(code, List(message))))

    def accepted(id: String): F[Response[F]] = Accepted(Json.obj("instanceId" -> id.asJson))

    /** What happens first: a `core.respond` node finished, or the instance ended. */
    sealed trait Outcome
    final case class Responded(node: String) extends Outcome
    final case class Ended(status: String) extends Outcome

    def await(id: String, timeout: FiniteDuration): F[Option[Outcome]] =
      engine
        .events(id)
        .collect[Outcome] {
          case e: AuditEntry.NodeFinished if e.nodeKind == RespondNode.Kind && e.status == "succeeded" => Responded(e.node)
          case e: AuditEntry.InstanceFinished                                                           => Ended(e.status)
        }
        .take(1)
        .compile
        .last
        .timeoutTo(timeout, Async[F].pure(None))

    /** The response that the node `nodeId` describes, built from the state of the instance. */
    def respond(id: String, nodeId: String): F[Response[F]] =
      engine.status(id).flatMap {
        case None => problem(Status.InternalServerError, "internal-error", s"instance '$id' disappeared")
        case Some(s) =>
          engine.store.get(s.flowId, Some(s.revision)).flatMap { flow =>
            flow.flatMap(_.nodes.find(_.id == nodeId)).map(_.params.as[RespondNode.Params]) match {
              case Some(Right(p)) =>
                val status = Status.fromInt(p.status).getOrElse(Status.Ok)
                val body = p.body.flatMap(_.get(s.state))
                val headers = p.headers.toList.map { case (k, v) => Header.Raw(CIString(k), v) }
                val raw = headers.exists(h => h.name == CIString("Content-Type") && !h.value.toLowerCase.contains("json"))
                val base = Response[F](status).putHeaders(headers)
                (body, raw) match {
                  case (None, _)                    => Async[F].pure(base)
                  case (Some(b), true) if b.isString => Async[F].pure(base.withBodyStream(fs2.Stream.emits(b.asString.get.getBytes(UTF_8).toIndexedSeq).covary[F]))
                  case (Some(b), _)                 => Async[F].pure(base.withEntity(b))
                }
              case _ => problem(Status.InternalServerError, "internal-error", s"the respond node '$nodeId' is not in the flow")
            }
          }
      }

    def serve(trigger: TriggerDef, req: Request[F], path: String, body: Array[Byte]): F[Response[F]] = {
      val headers = req.headers.headers.map(h => h.name.toString.toLowerCase -> h.value).toMap
      val parsed: Json =
        if (body.isEmpty) Json.Null
        else io.circe.parser.parse(new String(body, UTF_8)).getOrElse(Json.fromString(new String(body, UTF_8)))
      val request = HookRequest.json(req.method.name, path, req.params, headers, parsed)
      val key = headers.get("idempotency-key").filter(_.trim.nonEmpty)
      val requestId = key.getOrElse(UUID.randomUUID().toString)
      val origin = Origin.Trigger(trigger.id, Some(requestId))
      val traceparent = tracing.flatMap(t => headers.get(Traces.Header).map(t -> _))
      // To go on the caller's trace, the instance needs its id before it starts.
      val instanceId = key.map(instanceIdOf(trigger, _)).orElse(traceparent.map(_ => UUID.randomUUID().toString))
      val start = engine.start(trigger.flowId, None, HookRequest.inputs(trigger, request), instanceId, origin)
      val traced = (traceparent, instanceId).tupled.fold(start) { case ((t, parent), id) =>
        t.link(id, parent) >> start.onError { case _ => t.unlink(id) }
      }
      traced.flatMap { id =>
        trigger.response match {
          case TriggerDef.ResponseMode.Async => accepted(id)
          case TriggerDef.ResponseMode.Sync =>
            await(id, trigger.timeoutMs.fold(DefaultTimeout)(_.millis)).flatMap {
              case None                  => accepted(id)
              case Some(Responded(node)) => respond(id, node)
              case Some(Ended(status))   => Async[F].pure(Response[F](if (status == "succeeded") Status.Ok else Status.InternalServerError).withEntity(Json.obj("instanceId" -> id.asJson, "status" -> status.asJson)))
            }
        }
      }
    }

    HttpRoutes.of[F] {
      case req if req.pathInfo.segments.headOption.exists(_.decoded() == "hooks") =>
        val path = req.pathInfo.segments.drop(1).map(_.decoded()).mkString("/")
        triggers.route(req.method.name, path).flatMap {
          case None => problem(Status.NotFound, "not-found", s"no trigger serves ${req.method.name} /hooks/$path")
          case Some(trigger) =>
            req.body.compile.to(Array).flatMap { body =>
              HookAuth.check[F](trigger.auth, name => req.headers.get(CIString(name)).map(_.head.value), body, secrets).flatMap {
                case false => problem(Status.Unauthorized, "unauthorized", "the request does not carry valid credentials")
                case true  => serve(trigger, req, path, body)
              }
            }
        }
    }
  }
}
