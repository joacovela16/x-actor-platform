package xplat.flow.stream

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import xplat.core.connectors.Outgoing
import xplat.flow.engine.{AuditTrail, FlowEngine}
import xplat.flow.model.FlowDef
import xplat.flow.nodes.Connections
import xplat.flow.queue.PublisherPool

import java.nio.charset.StandardCharsets.UTF_8

/** The dead letters of an instance, and publishing them again.
  *
  *   - `GET /api/instances/{id}/dead-letters`: every dead letter of the stream segments of the instance, numbered, with whether it
  *     can be published again.
  *   - `POST /api/instances/{id}/dead-letters/reinject` with `{"indexes": [...]}` (all of them if there are none): publishes the
  *     original message of each one again to the queue its source reads, so that the flow takes it again. Only an element that came
  *     from a `stream.source.queue` can be; the message carries `x-reinjected-from` = `<instance>:<node>`.
  */
object DeadLetterRoutes {

  /** A dead letter as the API answers it. */
  final case class Letter(index: Int, segment: String, letter: Json, sourceKind: Option[String], sourceParams: Option[Json]) {
    def origin: Option[Json] = letter.hcursor.downField("origin").focus
    def reinjectable: Boolean = sourceKind.contains(QueueSource.Kind) && origin.isDefined
    def toJson: Json =
      letter.deepMerge(Json.obj("index" -> index.asJson, "segment" -> segment.asJson, "reinjectable" -> reinjectable.asJson, "sourceKind" -> sourceKind.asJson))
  }

  /** The dead letters in the audit of an instance whose flow is `flow`. */
  def letters(audit: Json, flow: Option[FlowDef]): List[Letter] = {
    val entries = audit.hcursor.downField("entries").as[List[Json]].getOrElse(Nil)
    val found = entries.flatMap { e =>
      val c = e.hcursor
      val node = c.get[String]("node").orElse(c.get[String]("nodeId")).toOption
      val dead = c.downField("info").downField("deadLetters").as[List[Json]].getOrElse(Nil)
      node.toList.flatMap(n => dead.map(n -> _))
    }
    found.zipWithIndex.map { case ((segment, letter), i) =>
      val source = letter.hcursor.get[String]("source").toOption.flatMap(id => flow.flatMap(_.nodes.find(_.id == id)))
      Letter(i, segment, letter, source.map(_.kind), source.map(_.params))
    }
  }

  /** The message to publish again: the original element, as the source had read it. */
  def message(p: QueueSource.Params, origin: Json): Either[String, (Array[Byte], Option[String])] = {
    val body = if (p.envelope) origin.hcursor.downField("body").focus.toRight("the original element has no body") else Right(origin)
    val key = if (p.envelope) origin.hcursor.get[String]("key").toOption else None
    body.flatMap { b =>
      val text = if (p.format == "text") b.asString.toRight("the original element is not a text") else Right(b.noSpaces)
      text.map(t => (t.getBytes(UTF_8), key))
    }
  }

  def apply[F[_]](engine: FlowEngine[F], connections: Connections[F], pool: PublisherPool[F])(implicit F: Async[F]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    def load(id: String): F[Option[List[Letter]]] =
      engine.audit(id).flatMap {
        case None => F.pure(None)
        case Some(trail) =>
          engine.store.get(trail.instance.flowId, Some(trail.instance.revision)).map(flow => Some(letters(io.circe.Encoder[AuditTrail].apply(trail), flow)))
      }

    def reinject(id: String, l: Letter): F[Either[String, Unit]] =
      (l.origin, l.sourceParams.toRight("its source is not known").flatMap(QueueSource.params)) match {
        case (_, Left(problem)) => F.pure(Left(problem))
        case (None, _)          => F.pure(Left("it has no original element"))
        case (Some(origin), Right(p)) =>
          message(p, origin) match {
            case Left(problem) => F.pure(Left(problem))
            case Right((bytes, key)) =>
              connections.resolve(p.connection).flatMap {
                case None => F.pure(Left(s"the connection '${p.connection}' does not exist"))
                case Some(c) =>
                  pool
                    .use(c, p.source, p.destinationType)(_.publish(Outgoing(bytes, key, Map("x-reinjected-from" -> s"$id:${l.segment}"))))
                    .as(Right(()): Either[String, Unit])
                    .handleError(e => Left(Option(e.getMessage).getOrElse(e.toString)))
              }
          }
      }

    HttpRoutes.of[F] {
      case GET -> Root / "api" / "instances" / id / "dead-letters" =>
        load(id).flatMap {
          case None     => NotFound(Json.obj("error" -> s"instance '$id' does not exist".asJson))
          case Some(ls) => Ok(Json.fromValues(ls.map(_.toJson)))
        }

      case req @ POST -> Root / "api" / "instances" / id / "dead-letters" / "reinject" =>
        req.as[Json].handleError(_ => Json.obj()).flatMap { body =>
          val wanted = body.hcursor.get[List[Int]]("indexes").toOption.filter(_.nonEmpty)
          load(id).flatMap {
            case None => NotFound(Json.obj("error" -> s"instance '$id' does not exist".asJson))
            case Some(ls) =>
              val chosen = wanted.fold(ls)(w => ls.filter(l => w.contains(l.index)))
              chosen
                .traverse[F, (Int, Either[String, Unit])] { l =>
                  if (!l.reinjectable) F.pure(l.index -> (Left("it did not come from a queue source"): Either[String, Unit]))
                  else reinject(id, l).map(l.index -> _)
                }
                .flatMap { results =>
                  Ok(
                    Json.obj(
                      "reinjected" -> results.count(_._2.isRight).asJson,
                      "skipped" -> Json.fromValues(results.collect { case (i, Left(why)) => Json.obj("index" -> i.asJson, "reason" -> why.asJson) })
                    )
                  )
                }
          }
        }
    }
  }
}
