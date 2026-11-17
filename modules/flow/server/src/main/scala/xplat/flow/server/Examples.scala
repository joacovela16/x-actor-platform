package xplat.flow.server

import cats.effect.Async
import cats.syntax.all._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import xplat.flow.engine.FlowStore
import io.circe.Json
import xplat.flow.http.{ConnectionStore, TriggerDef, TriggerRegistry}
import xplat.flow.model.FlowDef

import java.nio.charset.StandardCharsets.UTF_8

/** Flows to try the server with, each installed when the server starts and its flow and trigger are not there yet, each with the
  * trigger that runs it. They call `jsonplaceholder.typicode.com`, a fake JSON API, so they need internet:
  *
  *  - `example-posts`: `GET /hooks/examples/posts?user=1` asks for posts, keeps a summary with `script.js` and answers JSON;
  *  - `example-user-card`: `GET /hooks/examples/user?id=3` asks for a user and their todos at the same time, joins them and answers
  *    an HTML page.
  *  - `example-order-in` and `example-order-process`: `POST /hooks/examples/orders` with an order validates it, gives it an id,
  *    publishes it to the Redis stream `orders` (connection `orders-queue`, `redis://localhost:6379`) and answers 202; a `queue`
  *    trigger reads the stream and `example-order-process` prices it and reserves its stock at the same time, then plans the
  *    shipment. They need a Redis: `docker run -p 6379:6379 redis`.
  *
  * They are kept in `resources/examples`, one file per example: `{"flow": ..., "trigger": ..., "connection": ...}`; the connection,
  * optional, is created if there is none with its name.
  */
object Examples {

  val Files: List[String] = List("posts.json", "user-card.json", "order-in.json", "order-process.json")

  private final case class Example(flow: FlowDef, trigger: TriggerDef, connection: Option[(String, String, Json)])

  def install[F[_]](store: FlowStore[F], triggers: TriggerRegistry[F], connections: ConnectionStore[F])(implicit A: Async[F]): F[Unit] =
    for {
      log <- Slf4jLogger.fromName[F]("xplat.flow.examples")
      existing <- (store.list, triggers.list).mapN((f, t) => (f.map(_.id).toSet, t.map(_.id).toSet))
      (flows, trigs) = existing
      _ <-
          Files.traverse_ { name =>
            load[F](name).flatMap {
              case Example(flow, trigger, _) if flows(flow.id) || trigs(trigger.id) => A.unit
              case Example(flow, trigger, connection) =>
              connection.traverse_ { case (cname, kind, params) =>
                connections.get(cname).flatMap {
                  case Some(_) => A.unit
                  case None =>
                    connections.put(cname, kind, params, Map.empty).flatMap {
                      case Left(problems) => log.warn(s"the connection '$cname' of the example '${flow.id}' is not valid: ${problems.mkString("; ")}")
                      case Right(_)       => log.info(s"connection '$cname' ($kind) created for the example '${flow.id}'")
                    }
                }
              } >> store.save(flow).flatMap {
                case Left(errors) => log.warn(s"the example '${flow.id}' is not valid: ${errors.map(_.message).mkString("; ")}")
                case Right(_) =>
                  triggers.put(trigger).flatMap {
                    case Left(problems) => log.warn(s"the trigger of the example '${flow.id}' is not valid: ${problems.mkString("; ")}")
                    case Right(t) if t.kind == TriggerDef.Kind.Http => log.info(s"example '${flow.id}' installed: ${t.httpMethod} /hooks/${t.route}")
                    case Right(t)                                   => log.info(s"example '${flow.id}' installed: reads ${t.queue.getOrElse("")} of ${t.connection.getOrElse("")}")
                  }
              }
            }.handleErrorWith(e => log.warn(e)(s"cannot install the example $name"))
          }
    } yield ()

  private def load[F[_]](name: String)(implicit A: Async[F]): F[Example] =
    A.blocking {
      val in = Option(getClass.getResourceAsStream(s"/examples/$name")).getOrElse(throw new IllegalStateException(s"no resource /examples/$name"))
      try new String(in.readAllBytes(), UTF_8) finally in.close()
    }.flatMap { text =>
      A.fromEither(io.circe.parser.parse(text).flatMap { j =>
        val c = j.hcursor
        val connection = c.downField("connection").success.traverse { cc =>
          (cc.get[String]("name"), cc.get[String]("kind"), cc.getOrElse[Json]("params")(Json.obj())).tupled
        }
        (c.get[FlowDef]("flow"), c.get[TriggerDef]("trigger"), connection).mapN(Example.apply)
      })
    }
}
