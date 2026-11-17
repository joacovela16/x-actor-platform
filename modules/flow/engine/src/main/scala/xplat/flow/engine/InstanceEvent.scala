package xplat.flow.engine

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import xplat.flow.engine.InstanceState.Status
import xplat.flow.model.{FlowDef, NodeOutput}

/** What happens in an instance, as the journal keeps it. The instance is rebuilt from these, and the audit trail is made of
  * them: each carries the time it happened (`at`, milliseconds since the epoch) and what the audit needs to explain it.
  */
sealed trait InstanceEvent {
  def at: Long
}

object InstanceEvent {

  /** The instance starts. `plan` is the flow as the engine runs it, when it differs from `flow`: a flow with streams has each
    * stream segment condensed into one node (see [[xplat.flow.model.StreamSegments]]). Without it, `flow` is run as it is.
    */
  final case class Started(flowId: String, revision: Int, flow: FlowDef, inputs: Json, origin: Origin, at: Long, plan: Option[FlowDef] = None) extends InstanceEvent

  /** A node begins to run. `hash` is that of its configuration and of what it reads, and `reads` what it reads, as the audit
    * shows it (secrets hidden).
    */
  final case class NodeStarted(nodeId: String, attempt: Int, hash: String, reads: List[AuditEntry.Read], at: Long) extends InstanceEvent

  /** The node finished through `port`, and `values` are written in the state. `reused`: it did not run, the result of the
    * last run was taken again, with `hash` the one it was reused under. `info` is what the node says about this run.
    */
  final case class NodeSucceeded(
      nodeId: String,
      port: String,
      values: Map[String, Json],
      memory: Option[Json],
      reused: Boolean,
      at: Long,
      info: Map[String, Json] = Map.empty,
      hash: Option[String] = None
  ) extends InstanceEvent

  /** The node failed; the execution goes on through its error port. */
  final case class NodeFailed(nodeId: String, error: String, reused: Boolean, at: Long, info: Map[String, Json] = Map.empty, hash: Option[String] = None)
      extends InstanceEvent

  /** The node answered that it waits until `until` (milliseconds since the epoch) and then goes on with `output`. The node stays
    * running: when the time comes the engine finishes it with `output`, without running it again, and after a restart it sets the
    * timer again from this event.
    */
  final case class NodeSuspended(nodeId: String, attempt: Int, until: Long, output: NodeOutput, at: Long) extends InstanceEvent

  /** The node will not run, because none of the edges that lead to it fired, for these `causes`. */
  final case class NodeDead(nodeId: String, causes: List[AuditEntry.Cause], at: Long) extends InstanceEvent

  /** The node has some of its incoming edges and waits for these (`from:port` each). */
  final case class JoinWaiting(nodeId: String, pending: List[String], at: Long) extends InstanceEvent

  /** The instance runs again, asked for by `origin`. `flow` is the definition to use, the same or a newer revision. */
  final case class Retried(flow: FlowDef, origin: Origin, at: Long, plan: Option[FlowDef] = None) extends InstanceEvent

  final case class Finished(status: Status, at: Long) extends InstanceEvent

  private def tag(name: String, json: Json): Json = json.mapObject(_.add("type", Json.fromString(name)))

  private val started = deriveEncoder[Started]
  private val nodeStarted = deriveEncoder[NodeStarted]
  private val nodeSucceeded = deriveEncoder[NodeSucceeded]
  private val nodeFailed = deriveEncoder[NodeFailed]
  private val nodeSuspended = deriveEncoder[NodeSuspended]
  private val nodeDead = deriveEncoder[NodeDead]
  private val joinWaiting = deriveEncoder[JoinWaiting]
  private val retried = deriveEncoder[Retried]
  private val finished = deriveEncoder[Finished]

  implicit val encoder: Encoder[InstanceEvent] = Encoder.instance {
    case e: Started       => tag("started", started(e))
    case e: NodeStarted   => tag("node-started", nodeStarted(e))
    case e: NodeSucceeded => tag("node-succeeded", nodeSucceeded(e))
    case e: NodeFailed    => tag("node-failed", nodeFailed(e))
    case e: NodeSuspended => tag("node-suspended", nodeSuspended(e))
    case e: NodeDead      => tag("node-dead", nodeDead(e))
    case e: JoinWaiting   => tag("join-waiting", joinWaiting(e))
    case e: Retried       => tag("retried", retried(e))
    case e: Finished      => tag("finished", finished(e))
  }

  implicit val decoder: Decoder[InstanceEvent] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "started"        => deriveDecoder[Started].apply(c)
      case "node-started"   => deriveDecoder[NodeStarted].apply(c)
      case "node-succeeded" => deriveDecoder[NodeSucceeded].apply(c)
      case "node-failed"    => deriveDecoder[NodeFailed].apply(c)
      case "node-suspended" => deriveDecoder[NodeSuspended].apply(c)
      case "node-dead"      => deriveDecoder[NodeDead].apply(c)
      case "join-waiting"   => deriveDecoder[JoinWaiting].apply(c)
      case "retried"        => deriveDecoder[Retried].apply(c)
      case "finished"       => deriveDecoder[Finished].apply(c)
      case other            => Left(DecodingFailure(s"unknown event '$other'", c.history))
    }
  }
}
