package xplat.flow.http

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Cache-Control`
import org.http4s.{CacheDirective, Header, HttpRoutes, Request, Response, ServerSentEvent, Status}
import org.typelevel.ci._
import xplat.flow.engine._
import xplat.flow.model._

import scala.concurrent.duration._

/** `/api`: the catalog of nodes, the flows, their instances (with audit and live events) and tests.
  *
  * Drafts: `PUT /api/flows/{id}/draft` keeps the flow without validating it (and answers its errors), `GET` gives it back, `DELETE`
  * discards it, and `POST /api/flows/{id}/publish` validates it and saves it as a new revision, which removes the draft. A `PUT
  * /api/flows/{id}` that saves a revision removes the draft too. `GET /api/flows` lists the flows with `draft: true` when they have
  * one, and the flows that only have a draft with `revision: 0`.
  */
object FlowRoutes {

  def apply[F[_]: Async](engine: FlowEngine[F], deployments: Deployments[F], drafts: FlowDrafts[F]): HttpRoutes[F] = {
    val dsl = Http4sDsl[F]
    import dsl._

    val store = engine.store
    val catalog = engine.catalog

    def bad(message: String): F[Response[F]] = BadRequest(ApiJson.problems("bad-request", List(message)))
    def notFound(what: String): F[Response[F]] = NotFound(ApiJson.problems("not-found", List(what)))

    /** The resolver of the store with the draft on top, so a flow that calls itself is found before it is saved. */
    def resolverWith(draft: FlowDef): F[FlowResolver] =
      store.resolver.map { base =>
        new FlowResolver {
          def find(flowId: String, revision: Option[Int]): Option[FlowDef] =
            if (flowId == draft.id && revision.forall(_ == draft.revision)) Some(draft) else base.find(flowId, revision)
        }
      }

    def validate(flow: FlowDef): F[List[FlowError]] =
      (resolverWith(flow), engine.types).mapN((resolver, types) => FlowValidator.validate(flow, catalog, resolver, types))

    /** A draft as the editor gets it: the flow (without secrets) and what stops it from being published. */
    def draftJson(draft: FlowDef, errors: List[FlowError]): Json =
      Json.obj("flow" -> FlowSecrets.strip(draft, catalog).asJson).deepMerge(ApiJson.flowErrors(errors))

    def startInstance(flowId: String, body: Json): F[Response[F]] = {
      val c = body.hcursor
      (for {
        inputs <- c.getOrElse[Json]("inputs")(Json.obj())
        revision <- c.get[Option[Int]]("revision")
        instanceId <- c.get[Option[String]]("instanceId")
      } yield (inputs, revision, instanceId)).fold(
        e => bad(e.message),
        { case (inputs, revision, instanceId) =>
          engine.start(flowId, revision, inputs, instanceId, Origin.Manual()).flatMap(id => Accepted(Json.obj("instanceId" -> id.asJson)))
        }
      )
    }

    def search(req: Request[F]): F[Response[F]] = {
      val p = req.params
      def opt[A](name: String)(parse: String => Either[String, A]): Either[String, Option[A]] = p.get(name).filter(_.nonEmpty).traverse(parse)
      (for {
        status <- opt("status")(ApiJson.status)
        from <- opt("from")(ApiJson.time)
        to <- opt("to")(ApiJson.time)
        limit <- opt("limit")(s => s.toIntOption.filter(_ > 0).toRight(s"not a limit: '$s'"))
      } yield InstanceQuery(
        flowId = p.get("flowId").filter(_.nonEmpty),
        status = status,
        origin = p.get("origin").filter(_.nonEmpty),
        from = from,
        to = to,
        limit = limit.fold(50)(_ min 500),
        after = p.get("after").filter(_.nonEmpty)
      )).fold(bad, q => engine.search(q).flatMap(page => Ok(page.items.asJson).map(_.putHeaders(Header.Raw(ci"X-Next-After", page.next.getOrElse(""))))))
    }

    def events(id: String): F[Response[F]] =
      engine.status(id).flatMap {
        case None => notFound(s"instance '$id' does not exist")
        case Some(_) =>
          val entries: Stream[F, ServerSentEvent] =
            engine.events(id).map(e => ServerSentEvent(data = Some(ApiJson.event(e).noSpaces), id = Some(ServerSentEvent.EventId(e.seq.toString))))
          // A comment now and then keeps proxies from closing a stream that is waiting.
          val heartbeat: Stream[F, ServerSentEvent] = Stream.awakeEvery[F](15.seconds).as(ServerSentEvent(comment = Some("keep-alive")))
          Ok(entries.mergeHaltL(heartbeat)).map(_.putHeaders(`Cache-Control`(CacheDirective.`no-cache`()), Header.Raw(ci"X-Accel-Buffering", "no")))
      }

    /** Whether the flow is enabled and what its cron nodes would do: the times they run at and the next one. */
    def schedule(flow: FlowDef): F[Response[F]] =
      for {
        enabled <- deployments.isEnabled(flow.id)
        now <- Async[F].realTime.map(_.toMillis)
        response <- Ok(ApiJson.schedule(flow, enabled, now))
      } yield response

    def test(flowId: String, body: Json): F[Response[F]] = {
      val c = body.hcursor
      (for {
        draft <- c.get[Option[FlowDef]]("flow").leftMap(_.message)
        inputs <- c.getOrElse[Json]("inputs")(Json.obj()).leftMap(_.message)
        mocks <- c.getOrElse[Map[String, Json]]("mocks")(Map.empty).leftMap(_.message).flatMap(_.toList.traverse { case (n, m) => ApiJson.mock(m).map(n -> _) }).map(_.toMap)
        realReads <- c.getOrElse[Boolean]("realReads")(false).leftMap(_.message)
      } yield (draft, inputs, mocks, realReads)).fold(
        bad,
        { case (draft, inputs, mocks, realReads) =>
          draft.map(_.copy(id = flowId)).map(f => f.some.pure[F]).getOrElse(store.get(flowId, None)).flatMap {
            case None       => notFound(s"flow '$flowId' does not exist")
            case Some(flow) => engine.simulate(flow, inputs, mocks, realReads).flatMap(Ok(_))
          }
        }
      )
    }

    HttpRoutes.of[F] {
      case GET -> Root / "api" / "catalog" =>
        // The node that stands for a stream segment is made by the engine: it is not one for the palette.
        Ok(Json.fromValues(catalog.all.filterNot(_.kind == StreamSegments.SegmentKind).map(ApiJson.catalogEntry)))

      case req @ POST -> Root / "api" / "catalog" / kind / "describe" =>
        catalog.get(kind) match {
          case None    => notFound(s"node kind '$kind' does not exist")
          case Some(d) => req.as[Json].flatMap(params => Ok(ApiJson.describe(d, params)))
        }

      case GET -> Root / "api" / "flows" =>
        (store.list, deployments.enabled, drafts.all).mapN { (published, on, pending) =>
          val withDraft = pending.map(_.id).toSet
          val listed = published.map(f => f.id -> f.asJson.mapObject(_.add("enabled", Json.fromBoolean(on(f.id))).add("draft", Json.fromBoolean(withDraft(f.id)))))
          val onlyDraft = pending.filterNot(d => published.exists(_.id == d.id)).map { d =>
            d.id -> Json.obj("id" -> Json.fromString(d.id), "name" -> Json.fromString(d.name), "revision" -> Json.fromInt(0), "enabled" -> Json.False, "draft" -> Json.True)
          }
          Json.fromValues((listed ++ onlyDraft).sortBy(_._1).map(_._2))
        }.flatMap(Ok(_))

      case GET -> Root / "api" / "flows" / id / "draft" =>
        drafts.get(id).flatMap {
          case None        => notFound(s"flow '$id' has no draft")
          case Some(draft) => validate(draft).flatMap(errors => Ok(draftJson(draft, errors)))
        }

      case req @ PUT -> Root / "api" / "flows" / id / "draft" =>
        req.as[Json].flatMap { body =>
          ApiJson.decode[FlowDef](body) match {
            case Left(e) => bad(e)
            case Right(sent) =>
              for {
                // The secrets the editor does not get back are kept from the draft before, or else from the published flow.
                previous <- drafts.get(id).flatMap(d => d.fold(store.get(id, None))(f => Async[F].pure(Some(f))))
                draft = FlowSecrets.keep(sent.copy(id = id), previous, catalog)
                _ <- drafts.put(draft)
                errors <- validate(draft)
                response <- Ok(draftJson(draft, errors))
              } yield response
          }
        }

      case DELETE -> Root / "api" / "flows" / id / "draft" => drafts.delete(id) >> NoContent()

      case POST -> Root / "api" / "flows" / id / "publish" =>
        drafts.get(id).flatMap {
          case None => notFound(s"flow '$id' has no draft")
          case Some(draft) =>
            store.save(draft).flatMap {
              case Right(f)     => drafts.delete(id) >> Ok(FlowSecrets.strip(f, catalog))
              case Left(errors) => UnprocessableContent(ApiJson.flowErrors(errors))
            }
        }

      case GET -> Root / "api" / "flows" / id / "schedule" =>
        store.get(id, None).flatMap {
          case None       => notFound(s"flow '$id' does not exist")
          case Some(flow) => schedule(flow)
        }

      case POST -> Root / "api" / "flows" / id / action if action == "enable" || action == "disable" =>
        store.get(id, None).flatMap {
          case None       => notFound(s"flow '$id' does not exist")
          case Some(flow) => deployments.set(id, action == "enable") >> schedule(flow)
        }

      case req @ GET -> Root / "api" / "flows" / id =>
        val revision = req.params.get("revision").filter(_.nonEmpty)
        revision.traverse(r => r.toIntOption.toRight(r)) match {
          case Left(r) => bad(s"not a revision: '$r'")
          case Right(rev) =>
            store.get(id, rev).flatMap {
              case None       => notFound(s"flow '$id' does not exist")
              case Some(flow) => Ok(FlowSecrets.strip(flow, catalog))
            }
        }

      case req @ PUT -> Root / "api" / "flows" / id =>
        req.as[Json].flatMap { body =>
          ApiJson.decode[FlowDef](body) match {
            case Left(e) => bad(e)
            case Right(sent) =>
              for {
                previous <- store.get(id, None)
                flow = FlowSecrets.keep(sent.copy(id = id), previous, catalog)
                saved <- store.save(flow)
                response <- saved match {
                  case Right(f)     => drafts.delete(id) >> Ok(FlowSecrets.strip(f, catalog))
                  case Left(errors) => UnprocessableContent(ApiJson.flowErrors(errors))
                }
              } yield response
          }
        }

      case DELETE -> Root / "api" / "flows" / id =>
        (store.get(id, None), drafts.get(id)).tupled.flatMap {
          case (None, None) => notFound(s"flow '$id' does not exist")
          case _            => store.delete(id) >> drafts.delete(id) >> deployments.set(id, enabled = false) >> NoContent()
        }

      case req @ POST -> Root / "api" / "flows" / "validate" =>
        req.as[Json].flatMap { body =>
          ApiJson.decode[FlowDef](body) match {
            case Left(e)     => bad(e)
            case Right(flow) => validate(flow).flatMap(errors => Ok(ApiJson.flowErrors(errors)))
          }
        }

      case req @ POST -> Root / "api" / "flows" / id / "instances" =>
        req.bodyText.compile.string.flatMap { text =>
          if (text.trim.isEmpty) startInstance(id, Json.obj())
          else io.circe.parser.parse(text).fold(e => bad(e.message), startInstance(id, _))
        }

      case req @ POST -> Root / "api" / "flows" / id / "test" =>
        req.as[Json].flatMap(test(id, _))

      // The tests the flow keeps: those of the draft in the body (`{"flow": ...}`), or of the saved flow. For the editor and for CI.
      case req @ POST -> Root / "api" / "flows" / id / "tests" =>
        req.as[Json].handleError(_ => Json.obj()).flatMap { body =>
          body.hcursor.get[Option[FlowDef]]("flow").leftMap(_.message) match {
            case Left(problem) => bad(problem)
            case Right(draft) =>
              draft.map(_.copy(id = id)).map(f => f.some.pure[F]).getOrElse(store.get(id, None)).flatMap {
                case None       => notFound(s"flow '$id' does not exist")
                case Some(flow) => FlowTests.run(engine, flow).flatMap(r => Ok(FlowTests.answer(r)))
              }
          }
        }

      case req @ GET -> Root / "api" / "instances" => search(req)

      case GET -> Root / "api" / "instances" / id =>
        engine.status(id).flatMap {
          case None    => notFound(s"instance '$id' does not exist")
          case Some(s) => Ok(ApiJson.instanceStatus(s))
        }

      case req @ POST -> Root / "api" / "instances" / id / "retry" =>
        val latest = req.params.get("latest").contains("true")
        engine.retry(id, latest, None) >> Accepted(Json.obj("instanceId" -> id.asJson))

      // Stops an instance that is running (a stream that does not end stops too). One that has ended is left as it is.
      case POST -> Root / "api" / "instances" / id / "cancel" =>
        engine.cancel(id) >> Accepted(Json.obj("instanceId" -> id.asJson))

      case GET -> Root / "api" / "instances" / id / "audit" =>
        engine.audit(id).flatMap {
          case None    => notFound(s"instance '$id' does not exist")
          case Some(t) => Ok(t)
        }

      case GET -> Root / "api" / "instances" / id / "events" => events(id)
    }
  }
}
