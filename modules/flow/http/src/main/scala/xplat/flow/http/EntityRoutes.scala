package xplat.flow.http

import cats.effect.Async
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{DecodingFailure, Json}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}
import xplat.flow.engine.FlowEngine
import xplat.flow.entity.{EntityCatalog, EntityDef, EntityError}
import xplat.flow.model.{FlowError, FlowValidator}

/** `/api/entities`: the entities of the business model.
  *
  *   - `GET /api/entities`: `[{name, kind, label, revision, source, fields}]`; `source` is `store`, `code` or `plugin:<name>`.
  *     With `?full=true`, every entity whole, each with its `source`.
  *   - `GET /api/entities/{name}`: the entity, with its `source`. `GET /api/entities/{name}/revisions`: every revision.
  *   - `PUT /api/entities/{name}` body an entity: saves the next revision (the name is the one of the path). 200 `{entity, affected}`,
  *     where `affected` are the flows that validated before and do not now, `[{flowId, errors}]` (a warning: the entity is saved).
  *     422 `{errors}` if it is not valid; 409 if it is defined in code.
  *   - `DELETE /api/entities/{name}`: 204; 409 if it is defined in code or another entity or a flow uses it (`errors` says which).
  *   - `POST /api/entities/validate` body an entity: `{errors}`.
  *   - `POST /api/entities/{name}/check` body `{value, transition?, entity?}`: the value as the entity makes it (`entity` is a
  *     definition that is not saved yet, to try it): `{ok, value, problems: [{path, rule, message}]}`.
  */
object EntityRoutes {

  def apply[F[_]: Async](engine: FlowEngine[F], entities: EntityStore[F]): HttpRoutes[F] = {
    val dsl = Http4sDsl[F]
    import dsl._

    def invalid(errors: List[EntityError]): F[Response[F]] = UnprocessableContent(Json.obj("errors" -> errors.asJson))
    def conflict(code: String, message: String): F[Response[F]] = Conflict(ApiJson.problems(code, List(message)))

    /** A definition that does not decode is invalid, and says where. */
    def decode(body: Json, name: Option[String]): Either[EntityError, EntityDef] = {
      val named = name.fold(body)(n => body.mapObject(_.add("name", Json.fromString(n))))
      named.as[EntityDef].left.map { (f: DecodingFailure) =>
        EntityError("invalid-entity", name.getOrElse(body.hcursor.get[String]("name").getOrElse("")), f.getMessage)
      }
    }

    /** The latest revision of each flow with what is wrong with it among `catalog`. */
    def flowErrors(catalog: EntityCatalog): F[Map[String, List[FlowError]]] =
      for {
        summaries <- engine.store.list
        flows <- summaries.traverse(s => engine.store.get(s.id, None))
        resolver <- engine.store.resolver
      } yield flows.flatten.map(f => f.id -> FlowValidator.validate(f, engine.catalog, resolver, catalog)).toMap

    def put(name: String, body: Json): F[Response[F]] =
      decode(body, Some(name)) match {
        case Left(error) => invalid(List(error))
        case Right(entity) =>
          for {
            before <- entities.catalog
            was <- flowErrors(before)
            result <- entities.save(entity)
            response <- result match {
              case Left(EntityStore.Refused.ReadOnly(n, source)) => conflict("read-only", s"the entity '$n' is defined in $source and cannot be changed here")
              case Left(EntityStore.Refused.Invalid(errors))     => invalid(errors)
              case Left(other)                                   => conflict("refused", other.toString)
              case Right(saved) =>
                entities.catalog.flatMap(flowErrors).flatMap { now =>
                  val affected = now.toList.sortBy(_._1).collect {
                    case (flowId, errors) if errors.nonEmpty && was.get(flowId).forall(_.isEmpty) =>
                      Json.obj("flowId" -> flowId.asJson, "errors" -> errors.asJson)
                  }
                  Ok(Json.obj("entity" -> saved.asJson, "affected" -> Json.fromValues(affected)))
                }
            }
          } yield response
      }

    def delete(name: String): F[Response[F]] =
      entities.catalog.flatMap { catalog =>
        // A flow uses the entity if it no longer validates without it.
        flowErrors(catalog.without(name)).flatMap { without =>
          val flows = without.toList.collect { case (id, errors) if errors.exists(isAbout(name)) => id }.sorted
          if (flows.nonEmpty && catalog.get(name).isDefined)
            conflict("in-use", s"the entity '$name' is used by the flows ${flows.mkString(", ")}")
          else
            entities.delete(name).flatMap {
              case Right(_)                                       => NoContent()
              case Left(EntityStore.Refused.NotFound(n))          => NotFound(ApiJson.problems("not-found", List(s"the entity '$n' does not exist")))
              case Left(EntityStore.Refused.ReadOnly(n, source))  => conflict("read-only", s"the entity '$n' is defined in $source and cannot be deleted here")
              case Left(EntityStore.Refused.InUse(n, users))      => conflict("in-use", s"the entity '$n' is used by the entities ${users.mkString(", ")}")
              case Left(other)                                    => conflict("refused", other.toString)
            }
        }
      }

    def check(name: String, body: Json): F[Response[F]] = {
      val c = body.hcursor
      val draft = c.downField("entity").focus.filterNot(_.isNull).traverse(decode(_, Some(name)))
      (c.getOrElse[Json]("value")(Json.obj()), c.get[Option[String]]("transition"), draft) match {
        case (Left(e), _, _)        => BadRequest(ApiJson.problems("bad-request", List(e.message)))
        case (_, Left(e), _)        => BadRequest(ApiJson.problems("bad-request", List(e.message)))
        case (_, _, Left(error))    => invalid(List(error))
        case (Right(value), Right(transition), Right(definition)) =>
          entities.catalog.flatMap { saved =>
            val catalog = definition.fold(saved)(saved.updated)
            if (catalog.get(name).isEmpty) NotFound(ApiJson.problems("not-found", List(s"the entity '$name' does not exist")))
            else {
              val (made, problems) = transition match {
                case Some(t) => catalog.transition(name, value, t).fold(ps => (value, ps), v => (v, Nil))
                case None    => val checked = catalog.check(name, value); (checked.value, checked.problems)
              }
              Ok(Json.obj("ok" -> problems.isEmpty.asJson, "value" -> made, "problems" -> problems.asJson))
            }
          }
      }
    }

    HttpRoutes.of[F] {
      case req @ GET -> Root / "api" / "entities" =>
        if (req.params.get("full").contains("true"))
          entities.all.flatMap(all => Ok(all.map { case (e, source) => e.asJson.mapObject(_.add("source", Json.fromString(source))) }))
        else entities.list.flatMap(Ok(_))

      case req @ POST -> Root / "api" / "entities" / "validate" =>
        req.as[Json].flatMap { body =>
          decode(body, None) match {
            case Left(error)   => Ok(Json.obj("errors" -> List(error).asJson))
            case Right(entity) => entities.validate(entity).flatMap(errors => Ok(Json.obj("errors" -> errors.asJson)))
          }
        }

      case GET -> Root / "api" / "entities" / name =>
        entities.get(name).flatMap {
          case None                   => NotFound(ApiJson.problems("not-found", List(s"the entity '$name' does not exist")))
          case Some((entity, source)) => Ok(entity.asJson.mapObject(_.add("source", Json.fromString(source))))
        }

      case GET -> Root / "api" / "entities" / name / "revisions" => entities.revisions(name).flatMap(Ok(_))

      case req @ PUT -> Root / "api" / "entities" / name => req.as[Json].flatMap(put(name, _))

      case DELETE -> Root / "api" / "entities" / name => delete(name)

      case req @ POST -> Root / "api" / "entities" / name / "check" => req.as[Json].flatMap(check(name, _))
    }
  }

  private def isAbout(entity: String)(error: FlowError): Boolean = error match {
    case FlowError.UnknownEntity(_, e, _) => e == entity
    case _                                => false
  }
}
