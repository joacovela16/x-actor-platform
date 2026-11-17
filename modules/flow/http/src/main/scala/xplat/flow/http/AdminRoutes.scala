package xplat.flow.http

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}
import xplat.flow.engine.FlowEngine

/** `/api/triggers`, `/api/connections` and `/api/connection-kinds`. Secrets go in and never come out.
  *
  * With the pool of the server, also `GET /api/connection-clients` (the clients open now, by connection, never with their
  * configuration) and `POST /api/connections/{name}/close` (closes the clients of a connection; the next use opens a new one).
  *
  * `GET /api/plugins`: the plugins found when the server started, loaded or not, with their kinds or why they failed.
  */
object AdminRoutes {

  def apply[F[_]: Async](
      engine: FlowEngine[F],
      triggers: TriggerRegistry[F],
      connections: ConnectionStore[F],
      clients: Option[xplat.flow.nodes.ClientPool[F]] = None,
      plugins: Json = Json.arr()
  ): HttpRoutes[F] = {
    val dsl = Http4sDsl[F]
    import dsl._

    def invalid(code: String, problems: List[String]): F[Response[F]] = UnprocessableContent(ApiJson.problems(code, problems))

    def putTrigger(id: String, body: Json): F[Response[F]] =
      ApiJson.decode[TriggerDef](body) match {
        case Left(e) => BadRequest(ApiJson.problems("bad-request", List(e)))
        case Right(sent) =>
          val trigger = sent.copy(id = id)
          engine.store.get(trigger.flowId, None).flatMap {
            case None => invalid("invalid-trigger", List(s"the flow '${trigger.flowId}' does not exist"))
            case Some(_) =>
              triggers.put(trigger).flatMap {
                case Right(saved)   => Ok(saved)
                case Left(problems) => invalid("invalid-trigger", problems)
              }
          }
      }

    def putConnection(name: String, body: Json): F[Response[F]] = {
      val c = body.hcursor
      (for {
        kind <- c.get[String]("kind")
        params <- c.getOrElse[Json]("params")(Json.obj())
        secrets <- c.getOrElse[Map[String, String]]("secrets")(Map.empty)
      } yield (kind, params, secrets)).fold(
        e => BadRequest(ApiJson.problems("bad-request", List(e.message))),
        { case (kind, params, secrets) =>
          if (!name.matches("[A-Za-z0-9_-]+")) invalid("invalid-connection", List("the name may only have letters, digits, '_' and '-'"))
          else
            connections.put(name, kind, params, secrets).flatMap {
              case Right(view)    => Ok(view)
              case Left(problems) => invalid("invalid-connection", problems)
            }
        }
      )
    }

    HttpRoutes.of[F] {
      case GET -> Root / "api" / "plugins" => Ok(plugins)

      case GET -> Root / "api" / "triggers" => triggers.list.flatMap(l => Ok(l.asJson))

      case GET -> Root / "api" / "triggers" / id =>
        triggers.get(id).flatMap {
          case Some(t) => Ok(t)
          case None    => NotFound(ApiJson.problems("not-found", List(s"trigger '$id' does not exist")))
        }

      case req @ PUT -> Root / "api" / "triggers" / id => req.as[Json].flatMap(putTrigger(id, _))

      case DELETE -> Root / "api" / "triggers" / id =>
        triggers.delete(id).flatMap {
          case true  => NoContent()
          case false => NotFound(ApiJson.problems("not-found", List(s"trigger '$id' does not exist")))
        }

      case GET -> Root / "api" / "connection-kinds" => Ok(connections.kinds.asJson)

      case GET -> Root / "api" / "connections" => connections.list.flatMap(l => Ok(l.asJson))

      case GET -> Root / "api" / "connection-clients" =>
        clients.fold(Async[F].pure(List.empty[xplat.flow.nodes.ClientPool.Client]))(_.clients).flatMap { list =>
          Ok(Json.fromValues(list.map { c =>
            Json.obj(
              "connection" -> Json.fromString(c.connection),
              "kind" -> Json.fromString(c.kind),
              "extra" -> Json.fromString(c.extra),
              "inUse" -> Json.fromInt(c.inUse),
              "idleMillis" -> Json.fromLong(c.idleMillis),
              "retired" -> Json.fromBoolean(c.retired),
              "opening" -> Json.fromBoolean(c.opening)
            )
          }))
        }

      case POST -> Root / "api" / "connections" / name / "close" =>
        connections.get(name).flatMap {
          case None    => NotFound(ApiJson.problems("unknown-connection", List(s"there is no connection '$name'")))
          case Some(_) => clients.traverse_(_.close(name)) >> NoContent()
        }

      case req @ PUT -> Root / "api" / "connections" / name => req.as[Json].flatMap(putConnection(name, _))

      case DELETE -> Root / "api" / "connections" / name =>
        connections.delete(name).flatMap {
          case true  => NoContent()
          case false => NotFound(ApiJson.problems("not-found", List(s"connection '$name' does not exist")))
        }
    }
  }
}
