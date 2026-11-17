package xplat.flow.engine

import io.circe.{Encoder, Json}
import xplat.flow.engine.InstanceState.Status

/** What a simulation did, node by node.
  *
  * @param steps
  *   in the order they happened.
  * @param state
  *   the global state at the end.
  * @param audit
  *   the same audit trail that a real instance leaves, with each node marked `simulated` and the mock that answered it.
  */
final case class Trace(status: Status, steps: List[Trace.Step], state: Json, audit: List[AuditEntry] = Nil)

object Trace {

  /** @param outcome
    *   `ran` (the node really ran), `mocked` (a mock answered), `simulated` (the node has effects outside, so a default
    *   answer was used), `skipped` (it was not reached) or `failed` (it failed: see `error`).
    * @param state
    *   the global state after this step.
    * @param info
    *   what the node said about this run: for a stream segment, the metrics of each of its nodes with a sample of its elements,
    *   and its dead letters.
    */
  final case class Step(nodeId: String, kind: String, outcome: String, port: Option[String], values: Map[String, Json], error: Option[String], state: Json, info: Map[String, Json] = Map.empty)

  implicit val stepEncoder: Encoder[Step] = Encoder.instance { s =>
    Json
      .obj(
        "nodeId" -> Json.fromString(s.nodeId),
        "kind" -> Json.fromString(s.kind),
        "outcome" -> Json.fromString(s.outcome),
        "port" -> Encoder[Option[String]].apply(s.port),
        "values" -> Encoder[Map[String, Json]].apply(s.values),
        "error" -> Encoder[Option[String]].apply(s.error),
        "mocked" -> Json.fromBoolean(s.outcome == "mocked"),
        "simulated" -> Json.fromBoolean(s.outcome == "mocked" || s.outcome == "simulated"),
        "state" -> s.state,
        "info" -> (if (s.info.isEmpty) Json.Null else Encoder[Map[String, Json]].apply(s.info))
      )
      .mapObject(_.filter { case (_, v) => !v.isNull })
  }

  implicit val encoder: Encoder[Trace] = Encoder.instance { t =>
    Json.obj(
      "status" -> Encoder[Status].apply(t.status),
      "steps" -> Encoder[List[Step]].apply(t.steps),
      "state" -> t.state,
      // Every entry of the trail of a simulation says so.
      "audit" -> Json.fromValues(t.audit.map(e => Encoder[AuditEntry].apply(e).mapObject(_.add("simulated", Json.True))))
    )
  }
}
