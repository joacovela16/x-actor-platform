package xplat.flow.engine

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, DecodingFailure, Encoder, Json}

/** One line of the audit trail of an instance. They come in order, numbered by `seq` from 0, and are the same whether they
  * are read afterwards or followed live. Values that are secret have been replaced by `{"$redacted": true}` before an entry
  * is made.
  *
  * Every entry says when it happened (`at`, milliseconds since the epoch) and what kind it is (`kind`). Where an entry has
  * an `origin`, the JSON has it as the short label (`trigger:orders`) and the structured one as `originDetail`.
  */
sealed trait AuditEntry {
  def seq: Long
  def at: Long

  /** `instance-started`, `node-started`, `node-finished`, `node-skipped`, `node-waiting`, `edge-fired`, `edge-dead`,
    * `state-changed`, `retry-requested` or `instance-finished`.
    */
  def kind: String

  /** The node this is about, and the attempt, if it is about one. */
  def nodeOf: Option[String] = None
  def attemptOf: Option[Int] = None

  def withSeq(n: Long): AuditEntry
}

object AuditEntry {

  /** A value that was read from the state. `value` is empty if it was not there. */
  final case class Read(path: String, value: Option[Json])

  /** A value that a node wrote in the state, which replaced `previous`. */
  final case class Written(name: String, path: String, previous: Option[Json], value: Json)

  /** An edge: the port of `from` that it leaves, and the node it leads to. */
  final case class EdgeRef(from: String, port: String, to: String)

  /** Why an edge did not fire: its source did not run (`source-not-run`) or left through another port (`other-port`). In
    * that case `took` is the port it took and `decidedWith` what it had read, which is the value that made it decide.
    */
  final case class Cause(from: String, port: String, why: String, took: Option[String], decidedWith: List[Read])

  /** What the engine decided about a node that did not run, or has not run yet. Its `reason` is written in the JSON. */
  sealed trait Decision {
    def reason: String
  }
  object Decision {

    /** Its run policy said not to run it again: the result of its last run is taken as it was. */
    final case class RunPolicy(rule: String, port: String, outputs: List[Written], error: Option[String]) extends Decision {
      val reason = "skipped-by-run-policy"
    }

    /** None of the edges that lead to it fired. */
    final case class DeadBranch(causes: List[Cause]) extends Decision {
      val reason = "skipped-dead-branch"
    }

    /** It has some of its incoming edges and waits for the `pending` ones. */
    final case class WaitingJoin(pending: List[EdgeRef]) extends Decision {
      val reason = "waiting-join"
    }

    /** It is a timer: it waits until `fireAt` (milliseconds since the epoch) and then goes on. It is recorded, so it survives a
      * restart.
      */
    final case class Scheduled(fireAt: Long) extends Decision {
      val reason = "scheduled"
    }
  }

  /** In a simulation: how the node was answered (`ran`, `mocked` or `simulated`) and the mock that answered, if any. */
  final case class Simulated(outcome: String, mock: Option[Mock])

  final case class InstanceStarted(seq: Long, at: Long, instanceId: String, flowId: String, revision: Int, flowHash: String, origin: Origin, inputs: Json) extends AuditEntry {
    val kind = "instance-started"
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  final case class NodeStarted(
      seq: Long,
      at: Long,
      node: String,
      nodeKind: String,
      attempt: Int,
      paramsHash: String,
      params: Json,
      reads: List[Read],
      memory: Json
  ) extends AuditEntry {
    val kind = "node-started"
    override def nodeOf: Option[String] = Some(node)
    override def attemptOf: Option[Int] = Some(attempt)
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  /** @param status
    *   `succeeded` or `failed`.
    * @param childInstanceId
    *   the instance that ran the flow, if the node runs another flow: its audit trail is that instance's.
    * @param simulation
    *   in a simulation, how the node was answered.
    */
  final case class NodeFinished(
      seq: Long,
      at: Long,
      node: String,
      nodeKind: String,
      attempt: Int,
      startedAt: Long,
      durationMs: Long,
      status: String,
      port: String,
      error: Option[String],
      outputs: List[Written],
      memoryAfter: Option[Json],
      childInstanceId: Option[String],
      simulation: Option[Simulated],
      info: Map[String, Json] = Map.empty
  ) extends AuditEntry {
    val kind = "node-finished"
    override def nodeOf: Option[String] = Some(node)
    override def attemptOf: Option[Int] = Some(attempt)
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  /** A node that did not run; `decision` says why (`skipped-by-run-policy` or `skipped-dead-branch`). */
  final case class NodeSkipped(seq: Long, at: Long, node: String, nodeKind: String, attempt: Int, decision: Decision) extends AuditEntry {
    val kind = "node-skipped"
    override def nodeOf: Option[String] = Some(node)
    override def attemptOf: Option[Int] = Some(attempt)
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  /** A node that waits: `decision` is `waiting-join` with the edges it still needs, or `scheduled` with the time it will go on. */
  final case class NodeWaiting(seq: Long, at: Long, node: String, decision: Decision) extends AuditEntry {
    val kind = "node-waiting"
    override def nodeOf: Option[String] = Some(node)
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  final case class EdgeFired(seq: Long, at: Long, edge: EdgeRef) extends AuditEntry {
    val kind = "edge-fired"
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  final case class EdgeDead(seq: Long, at: Long, edge: EdgeRef, reason: String) extends AuditEntry {
    val kind = "edge-dead"
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  /** What a node changed in the global state, each value with what it replaced. It follows the entry of the node. */
  final case class StateChanged(seq: Long, at: Long, node: String, attempt: Int, changes: List[Written]) extends AuditEntry {
    val kind = "state-changed"
    override def nodeOf: Option[String] = Some(node)
    override def attemptOf: Option[Int] = Some(attempt)
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  final case class RetryRequested(seq: Long, at: Long, origin: Origin, revision: Int) extends AuditEntry {
    val kind = "retry-requested"
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  final case class InstanceFinished(seq: Long, at: Long, status: String, durationMs: Long) extends AuditEntry {
    val kind = "instance-finished"
    def withSeq(n: Long): AuditEntry = copy(seq = n)
  }

  // ---- JSON ----

  private def tagged(field: String, name: String, json: Json): Json = json.mapObject(_.add(field, Json.fromString(name)))

  implicit val readCodec: io.circe.Codec[Read] = io.circe.generic.semiauto.deriveCodec
  implicit val writtenCodec: io.circe.Codec[Written] = io.circe.generic.semiauto.deriveCodec
  implicit val edgeCodec: io.circe.Codec[EdgeRef] = io.circe.generic.semiauto.deriveCodec
  implicit val causeCodec: io.circe.Codec[Cause] = io.circe.generic.semiauto.deriveCodec
  implicit val simulatedCodec: io.circe.Codec[Simulated] = io.circe.generic.semiauto.deriveCodec

  implicit val decisionEncoder: Encoder[Decision] = Encoder.instance {
    case d: Decision.RunPolicy   => tagged("reason", d.reason, deriveEncoder[Decision.RunPolicy].apply(d))
    case d: Decision.DeadBranch  => tagged("reason", d.reason, deriveEncoder[Decision.DeadBranch].apply(d))
    case d: Decision.WaitingJoin => tagged("reason", d.reason, deriveEncoder[Decision.WaitingJoin].apply(d))
    case d: Decision.Scheduled   => tagged("reason", d.reason, deriveEncoder[Decision.Scheduled].apply(d))
  }
  implicit val decisionDecoder: Decoder[Decision] = Decoder.instance { c =>
    c.get[String]("reason").flatMap {
      case "skipped-by-run-policy" => deriveDecoder[Decision.RunPolicy].apply(c)
      case "skipped-dead-branch"   => deriveDecoder[Decision.DeadBranch].apply(c)
      case "waiting-join"          => deriveDecoder[Decision.WaitingJoin].apply(c)
      case "scheduled"             => deriveDecoder[Decision.Scheduled].apply(c)
      case other                   => Left(DecodingFailure(s"unknown decision '$other'", c.history))
    }
  }

  /** `origin` is the short label and `originDetail` the structured form. */
  private def withOrigin(json: Json, origin: Origin): Json =
    json.mapObject(_.add("origin", Json.fromString(origin.label)).add("originDetail", Encoder[Origin].apply(origin)))

  implicit val encoder: Encoder[AuditEntry] = Encoder.instance { e =>
    val json = e match {
      case e: InstanceStarted  => withOrigin(deriveEncoder[InstanceStarted].apply(e), e.origin)
      case e: NodeStarted      => deriveEncoder[NodeStarted].apply(e)
      case e: NodeFinished     => deriveEncoder[NodeFinished].apply(e)
      case e: NodeSkipped      => deriveEncoder[NodeSkipped].apply(e)
      case e: NodeWaiting      => deriveEncoder[NodeWaiting].apply(e)
      case e: EdgeFired        => deriveEncoder[EdgeFired].apply(e)
      case e: EdgeDead         => deriveEncoder[EdgeDead].apply(e)
      case e: StateChanged     => deriveEncoder[StateChanged].apply(e)
      case e: RetryRequested   => withOrigin(deriveEncoder[RetryRequested].apply(e), e.origin)
      case e: InstanceFinished => deriveEncoder[InstanceFinished].apply(e)
    }
    tagged("kind", e.kind, json)
  }

  implicit val decoder: Decoder[AuditEntry] = Decoder.instance { c =>
    // The label is only for reading; the structured origin is what makes the entry again.
    val c2 = c.downField("originDetail").focus.fold(c.value)(d => c.value.mapObject(_.add("origin", d).remove("originDetail"))).hcursor
    c2.get[String]("kind").flatMap {
      case "instance-started"  => deriveDecoder[InstanceStarted].apply(c2)
      case "node-started"      => deriveDecoder[NodeStarted].apply(c2)
      case "node-finished"     => deriveDecoder[NodeFinished].apply(c2)
      case "node-skipped"      => deriveDecoder[NodeSkipped].apply(c2)
      case "node-waiting"      => deriveDecoder[NodeWaiting].apply(c2)
      case "edge-fired"        => deriveDecoder[EdgeFired].apply(c2)
      case "edge-dead"         => deriveDecoder[EdgeDead].apply(c2)
      case "state-changed"     => deriveDecoder[StateChanged].apply(c2)
      case "retry-requested"   => deriveDecoder[RetryRequested].apply(c2)
      case "instance-finished" => deriveDecoder[InstanceFinished].apply(c2)
      case other               => Left(DecodingFailure(s"unknown entry '$other'", c2.history))
    }
  }
}
