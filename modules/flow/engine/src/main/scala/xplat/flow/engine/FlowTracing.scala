package xplat.flow.engine

import cats.effect.{Ref, Sync}
import cats.syntax.all._
import xplat.flow.nodes.Traces

import java.security.SecureRandom

/** The traces of the instances this server runs, made from the entries of their audit (a [[FlowObserver]]): a span for the
  * instance and one for each run of a node, with the times the audit recorded. When something finishes, its span goes to
  * `export` (the server turns them into OpenTelemetry and sends them by OTLP); nothing here depends on OpenTelemetry.
  *
  *  - An instance starts a new trace, unless it was [[link]]ed to a `traceparent` that came with what started it (an HTTP request,
  *    a message), or it is a subflow: then it goes under the span of the node that called it.
  *  - While a node runs, [[traceparent]] says where it is, for it to send on (the engine publishes the start of a node before it
  *    runs it, so it is always known by then).
  */
final class FlowTracing[F[_]] private (state: Ref[F, FlowTracing.State], export: List[FlowTracing.Span] => F[Unit])(implicit F: Sync[F])
    extends FlowObserver[F]
    with Traces[F] {

  import FlowTracing._

  /** The instance that `instanceId` will be goes under `traceparent`. Call it before starting it. Ignored if it is not valid. */
  def link(instanceId: String, traceparent: String): F[Unit] =
    parse(traceparent).traverse_(parent => state.update(s => s.copy(links = s.links.updated(instanceId, parent))))

  /** Forgets a [[link]] whose instance did not start. */
  def unlink(instanceId: String): F[Unit] = state.update(s => s.copy(links = s.links - instanceId))

  def traceparent(instanceId: String, nodeId: String): F[Option[String]] =
    state.get.map(s => (s.instances.get(instanceId), s.nodes.get(instanceId -> nodeId)).mapN((i, n) => format(i.traceId, n.spanId)))

  /** The span of an instance that runs, as a `traceparent`. */
  def instanceTraceparent(instanceId: String): F[Option[String]] = state.get.map(_.instances.get(instanceId).map(i => format(i.traceId, i.spanId)))

  def observe(instanceId: String, entry: AuditEntry): F[Unit] =
    F.delay(randomId(8)).flatMap { fresh =>
      F.delay(randomId(16)).flatMap { freshTrace =>
        state.modify(_.add(instanceId, entry, fresh, freshTrace)).flatMap(done => export(done).whenA(done.nonEmpty))
      }
    }
}

object FlowTracing {

  def make[F[_]: Sync](export: List[Span] => F[Unit]): F[FlowTracing[F]] = Ref.of[F, State](State()).map(new FlowTracing[F](_, export))

  /** A finished span. Ids are lower case hex (32 for a trace, 16 for a span); times are milliseconds since the epoch. */
  final case class Span(
      traceId: String,
      spanId: String,
      parentSpanId: Option[String],
      name: String,
      kind: Span.Kind,
      startMs: Long,
      endMs: Long,
      attributes: Map[String, String],
      error: Option[String]
  )

  object Span {
    sealed trait Kind
    /** The span of an instance: the flow serves something. */
    case object Server extends Kind
    /** The span of a node. */
    case object Internal extends Kind
  }

  /** Where a trace goes on: its trace and the span to hang from. */
  final case class Parent(traceId: String, spanId: String)

  private val Traceparent = "00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}".r

  def parse(traceparent: String): Option[Parent] = traceparent.trim.toLowerCase match {
    case Traceparent(t, s) if t != "0" * 32 && s != "0" * 16 => Some(Parent(t, s))
    case _                                                   => None
  }

  def format(traceId: String, spanId: String): String = s"00-$traceId-$spanId-01"

  private val random = new SecureRandom()

  private def randomId(bytes: Int): String = {
    val b = new Array[Byte](bytes)
    random.nextBytes(b)
    b.map(x => f"${x & 0xff}%02x").mkString
  }

  private[engine] final case class Open(traceId: String, spanId: String, parentSpanId: Option[String], flowId: String, revision: Int, origin: String, startMs: Long, error: Option[String])
  private[engine] final case class OpenNode(spanId: String, kind: String, attempt: Int, startMs: Long)

  private[engine] final case class State(
      links: Map[String, Parent] = Map.empty,
      instances: Map[String, Open] = Map.empty,
      nodes: Map[(String, String), OpenNode] = Map.empty
  ) {

    /** The state after `entry`, and the spans it finished. */
    def add(instanceId: String, entry: AuditEntry, fresh: String, freshTrace: String): (State, List[Span]) = entry match {
      case e: AuditEntry.InstanceStarted =>
        val parent = e.origin match {
          case Origin.Subflow(parentInstance, parentNode) =>
            (instances.get(parentInstance), nodes.get(parentInstance -> parentNode)).mapN((i, n) => Parent(i.traceId, n.spanId))
          case _ => links.get(instanceId)
        }
        val open = Open(parent.fold(freshTrace)(_.traceId), fresh, parent.map(_.spanId), e.flowId, e.revision, e.origin.label, e.at, None)
        (copy(links = links - instanceId, instances = instances.updated(instanceId, open)), Nil)

      case e: AuditEntry.NodeStarted =>
        if (!instances.contains(instanceId)) (this, Nil)
        else (copy(nodes = nodes.updated(instanceId -> e.node, OpenNode(fresh, e.nodeKind, e.attempt, e.at))), Nil)

      case e: AuditEntry.NodeFinished =>
        (instances.get(instanceId), nodes.get(instanceId -> e.node)) match {
          case (Some(i), Some(n)) =>
            val span = Span(
              i.traceId,
              n.spanId,
              Some(i.spanId),
              s"${e.nodeKind} ${e.node}",
              Span.Internal,
              n.startMs,
              math.max(e.at, n.startMs),
              Map(
                "flow.id" -> i.flowId,
                "flow.instance" -> instanceId,
                "flow.node" -> e.node,
                "flow.node.kind" -> e.nodeKind,
                "flow.node.attempt" -> e.attempt.toString,
                "flow.node.port" -> e.port
              ) ++ e.childInstanceId.map("flow.child_instance" -> _),
              if (e.status == "failed") Some(e.error.getOrElse("failed")) else None
            )
            val failed = span.error.map(m => s"${e.node}: $m")
            (copy(nodes = nodes - (instanceId -> e.node), instances = instances.updated(instanceId, i.copy(error = failed.orElse(i.error)))), List(span))
          case _ => (this, Nil)
        }

      case e: AuditEntry.InstanceFinished =>
        instances.get(instanceId) match {
          case None => (this, Nil)
          case Some(i) =>
            val span = Span(
              i.traceId,
              i.spanId,
              i.parentSpanId,
              s"flow ${i.flowId}",
              Span.Server,
              i.startMs,
              math.max(e.at, i.startMs),
              Map("flow.id" -> i.flowId, "flow.revision" -> i.revision.toString, "flow.instance" -> instanceId, "flow.origin" -> i.origin, "flow.status" -> e.status),
              if (e.status == "failed") Some(i.error.getOrElse("failed")) else None
            )
            (copy(instances = instances - instanceId, nodes = nodes.filterNot(_._1._1 == instanceId)), List(span))
        }

      case _ => (this, Nil)
    }
  }
}
