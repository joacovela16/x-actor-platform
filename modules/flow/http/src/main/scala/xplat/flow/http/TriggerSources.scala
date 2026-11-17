package xplat.flow.http

import cats.effect.{Async, Fiber, Ref, Resource}
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import xplat.flow.engine.{FlowEngine, FlowTracing, Origin}
import xplat.flow.nodes.{Connections, Traces, TriggerSource}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

/** Makes real the triggers that the plugins serve ([[TriggerSource]]): `queue` triggers today. For each trigger whose kind a source
  * serves and whose connection is of a kind it accepts, the source runs while the trigger exists, and starts an instance of its
  * flow for each thing that arrives.
  *
  *   - What arrives is a request; the `inputs` of the trigger map it to the inputs of the flow, as they do for HTTP.
  *   - The instance id is made of the trigger and the key the source gives (an idempotency key, or its place in the source), so
  *     something delivered again, because the server stopped before acknowledging it, does not start a second instance.
  *   - What cannot be turned into inputs for the flow will never work: it is logged and the source is told to drop it. Any other
  *     failure (the flow does not exist yet, the journal is down) fails the start, for the source to retry; if the source stops, it
  *     is started again after `restartAfter`.
  *   - Triggers and connections can change while it runs: every `pollEvery` the running sources are compared with the triggers,
  *     and the ones that were added, edited, deleted or whose connection changed are started, restarted or stopped.
  */
object TriggerSources {

  final case class Settings(pollEvery: FiniteDuration = 2.seconds, restartAfter: FiniteDuration = 5.seconds)

  private final case class Running[F[_]](fingerprint: Int, fiber: Fiber[F, Throwable, Unit])

  /** How a trigger served by a source is: `running`, `waiting` for its connection, or `unserved` (no source reads its kind of
    * connection).
    */
  final case class TriggerState(triggerId: String, kind: String, connection: Option[String], state: String)

  /** What the running sources are doing. */
  trait View[F[_]] {
    def states: F[List[TriggerState]]
  }

  object View {
    def none[F[_]](implicit F: cats.Applicative[F]): View[F] = new View[F] { def states: F[List[TriggerState]] = F.pure(Nil) }
  }

  /** The id of the instance that `key` starts for `trigger`. */
  def instanceId(trigger: TriggerDef, key: String): String = s"${trigger.id}-$key".replaceAll("[^A-Za-z0-9_.-]", "_")

  /** The key of a message: its `x-idempotency-key`, `idempotency-key` or `message-id` header, or else its place in the source. */
  def messageKey(source: String, position: String, headers: Map[String, String]): String =
    List("x-idempotency-key", "idempotency-key", "message-id").flatMap(headers.get).find(_.trim.nonEmpty).getOrElse(s"$source-$position")

  /** The request that a message makes, to map into inputs: its body is its JSON, or its text if it is not JSON. */
  def messageRequest(source: String, key: Option[String], position: String, headers: Map[String, String], value: Array[Byte]): Json = {
    val text = new String(value, UTF_8)
    val body = if (text.isEmpty) Json.Null else io.circe.parser.parse(text).getOrElse(Json.fromString(text))
    HookRequest.message(source, key, position, headers, body)
  }

  def resource[F[_]](
      engine: FlowEngine[F],
      triggers: TriggerRegistry[F],
      connections: Connections[F],
      sources: List[TriggerSource[F]],
      settings: Settings = Settings(),
      tracing: Option[FlowTracing[F]] = None
  )(implicit A: Async[F]): Resource[F, View[F]] =
    if (sources.isEmpty) Resource.pure(View.none[F])
    else
      for {
        log <- Resource.eval(Slf4jLogger.fromName[F]("xplat.flow.triggers"))
        running <- Resource.eval(Ref.of[F, Map[String, Running[F]]](Map.empty))
        seen <- Resource.eval(Ref.of[F, List[TriggerState]](Nil))
        _ <- Resource.onFinalize(running.get.flatMap(_.values.toList.traverse_(_.fiber.cancel)))
        _ <- A.background(new Reconciler[F](engine, triggers, connections, sources, settings, log, running, seen, tracing).forever)
      } yield new View[F] { def states: F[List[TriggerState]] = seen.get }

  private final class Reconciler[F[_]](
      engine: FlowEngine[F],
      triggers: TriggerRegistry[F],
      connections: Connections[F],
      sources: List[TriggerSource[F]],
      settings: Settings,
      log: Logger[F],
      running: Ref[F, Map[String, Running[F]]],
      seen: Ref[F, List[TriggerState]],
      tracing: Option[FlowTracing[F]]
  )(implicit A: Async[F]) {

    private val kinds: Set[String] = sources.map(_.kind).toSet

    def forever: F[Unit] = reconcile.handleErrorWith(e => log.error(e)("cannot reconcile the triggers")) >> A.sleep(settings.pollEvery) >> forever

    private def start(trigger: TriggerDef): TriggerSource.Start[F] = (request: Json, key: String, position: Option[String]) => {
      val id = instanceId(trigger, key)
      // A message that carries a `traceparent` header starts an instance on that trace.
      val parent = request.hcursor.downField("headers").get[String](Traces.Header).toOption
      val link = (tracing, parent).tupled.traverse_ { case (t, p) => t.link(id, p) }
      link >> engine
        .start(trigger.flowId, None, HookRequest.inputs(trigger, request), Some(id), Origin.Trigger(trigger.id, position))
        .onError { case _ => tracing.traverse_(_.unlink(id)) }
        .as(TriggerSource.Accepted: TriggerSource.Started)
        .recoverWith { case e: FlowEngine.InvalidInputs =>
          val where = position.fold("")(p => s" at $p")
          log.warn(s"what arrived$where for the trigger '${trigger.id}' does not fit the flow '${trigger.flowId}', so it is dropped: ${e.getMessage}")
            .as(TriggerSource.Rejected(e.getMessage))
        }
    }

    /** Runs the source, and starts it again if it stops on its own. */
    private def supervise(trigger: TriggerDef, connection: Connections.Resolved, source: TriggerSource[F]): F[Unit] = {
      val once = source.run(trigger.asJson, connection, start(trigger)) >> log.warn(s"the source of the trigger '${trigger.id}' ended")
      once.handleErrorWith(e => log.error(e)(s"the source of the trigger '${trigger.id}' stopped")) >> A.sleep(settings.restartAfter) >>
        supervise(trigger, connection, source)
    }

    private def stop(id: String): F[Unit] =
      running.modify(m => (m - id, m.get(id))).flatMap(_.traverse_(_.fiber.cancel))

    def reconcile: F[Unit] =
      for {
        desired <- triggers.list.map(_.filter(t => kinds.contains(t.kind.name)))
        resolved <- desired.traverse(t => t.connection.traverse(connections.resolve).map(r => t -> r.flatten))
        ids = desired.map(_.id).toSet
        current <- running.get
        _ <- current.keys.filterNot(ids).toList.traverse_(stop)
        _ <- seen.set(resolved.map { case (t, c) =>
          val state = c match {
            case None                                                                  => "waiting"
            case Some(conn) if sources.exists(s => s.kind == t.kind.name && s.accepts(conn.kind)) => "running"
            case Some(_)                                                               => "unserved"
          }
          TriggerState(t.id, t.kind.name, t.connection, state)
        })
        _ <- resolved.traverse_ {
          case (t, None) =>
            // Without its connection there is nothing to read from; it starts when the connection appears.
            stop(t.id) >> log.debug(s"the trigger '${t.id}' waits for the connection '${t.connection.getOrElse("")}'")
          case (t, Some(c)) =>
            sources.find(s => s.kind == t.kind.name && s.accepts(c.kind)) match {
              case None =>
                stop(t.id) >> log.debug(s"no plugin reads ${t.kind.name} triggers from a '${c.kind}' connection ('${t.id}')")
              case Some(source) =>
                val fingerprint = (t, c.kind, c.params.noSpaces, c.secrets.toList.sorted, sources.indexOf(source)).hashCode
                current.get(t.id) match {
                  case Some(r) if r.fingerprint == fingerprint => A.unit
                  case _ =>
                    stop(t.id) >> log.info(s"starting the source of the trigger '${t.id}' (${c.kind} '${t.queue.getOrElse("")}')") >>
                      A.start(supervise(t, c, source)).flatMap(fiber => running.update(_ + (t.id -> Running(fingerprint, fiber))))
                }
            }
        }
      } yield ()
  }
}
