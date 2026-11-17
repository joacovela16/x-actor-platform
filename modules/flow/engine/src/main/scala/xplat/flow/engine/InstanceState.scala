package xplat.flow.engine

import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Json}
import xplat.flow.model.{FlowDef, NodeOutput}

/** Everything about one execution of a flow. It is rebuilt by applying the [[InstanceEvent]]s in order.
  *
  * @param flow
  *   the definition being run, as it was when the instance started, so editing the flow does not change a run in progress.
  * @param inputs
  *   the values the instance was started with, by name of the input of the flow.
  * @param state
  *   the global state, shared by every node.
  * @param origin
  *   who started it, and `startedAt`/`endedAt` when (milliseconds since the epoch).
  */
final case class InstanceState(
    id: String,
    flow: Option[FlowDef] = None,
    inputs: Json = Json.obj(),
    status: InstanceState.Status = InstanceState.Status.Running,
    state: Json = Json.obj(),
    nodes: Map[String, InstanceState.NodeRun] = Map.empty,
    origin: Origin = Origin.Manual(),
    startedAt: Long = 0L,
    endedAt: Option[Long] = None,
    plan: Option[FlowDef] = None
) {

  /** The graph the engine runs: the plan if the flow has streams, else the flow itself. */
  def graph: Option[FlowDef] = plan.orElse(flow)

  def started: Boolean = flow.isDefined
  def finished: Boolean = started && status != InstanceState.Status.Running
}

object InstanceState {

  sealed trait Status { def name: String }
  object Status {
    case object Running extends Status { val name = "running" }
    case object Succeeded extends Status { val name = "succeeded" }
    case object Failed extends Status { val name = "failed" }
    implicit val codec: Codec[Status] = EnumCodec("status", List[Status](Running, Succeeded, Failed))(_.name)
  }

  sealed trait NodeStatus { def name: String }
  object NodeStatus {
    case object Pending extends NodeStatus { val name = "pending" }
    case object Running extends NodeStatus { val name = "running" }
    case object Succeeded extends NodeStatus { val name = "succeeded" }
    case object Failed extends NodeStatus { val name = "failed" }

    /** It will not run: none of the edges that lead to it fired. */
    case object Dead extends NodeStatus { val name = "dead" }
    implicit val codec: Codec[NodeStatus] = EnumCodec("node status", List[NodeStatus](Pending, Running, Succeeded, Failed, Dead))(_.name)
  }

  /** How a node went in the run before the last retry, which its run policy may reuse if what it depends on is the same. */
  final case class Last(succeeded: Boolean, port: String, outputs: Map[String, Json], error: Option[String], hash: String)
  object Last {
    implicit val codec: Codec[Last] = deriveCodec
  }

  /** A node that is waiting for a time: what it answers when it comes. */
  final case class Suspension(until: Long, output: NodeOutput)
  object Suspension {
    implicit val codec: Codec[Suspension] = deriveCodec
  }

  /** @param hash
    *   of the configuration of the node and of what it reads from the state when it started.
    * @param memory
    *   it survives retries.
    * @param reads
    *   what it read when it started, as the audit shows it, and `startedAt` when.
    * @param waitingOn
    *   the edges that its join is waiting for, as last recorded.
    * @param suspended
    *   the node is a timer that waits (it is still running); see [[InstanceEvent.NodeSuspended]].
    * @param info
    *   what the node said about its last run (for a stream segment, its metrics and dead letters).
    */
  final case class NodeRun(
      status: NodeStatus = NodeStatus.Pending,
      attempt: Int = 0,
      port: Option[String] = None,
      outputs: Map[String, Json] = Map.empty,
      error: Option[String] = None,
      memory: Json = Json.obj(),
      hash: Option[String] = None,
      last: Option[Last] = None,
      reads: List[AuditEntry.Read] = Nil,
      startedAt: Long = 0L,
      waitingOn: List[String] = Nil,
      suspended: Option[Suspension] = None,
      info: Option[Map[String, Json]] = None
  )
  object NodeRun {
    implicit val codec: Codec[NodeRun] = deriveCodec
  }

  implicit val codec: Codec[InstanceState] = deriveCodec
}
