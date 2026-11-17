package xplat.flow.http

import io.circe.syntax._
import io.circe.{Decoder, Json}
import xplat.flow.engine.InstanceState.Status
import xplat.flow.engine.{AuditEntry, CronSchedule, FlowSchedules, InstanceStatus, Mock}
import xplat.flow.model._

import java.time.Instant

/** The JSON of the API that is not just the codec of a type: what the UI expects when it differs from how the engine writes it. */
object ApiJson {

  /** The ports, with [[Port.Error]] at the end, which every node has. */
  def ports(list: List[Port]): Json = {
    val all = if (list.exists(_.name == Port.Error)) list else list :+ Port(Port.Error)
    all.asJson
  }

  def reads(list: List[StateRead]): Json =
    Json.fromValues(list.map(r => Json.obj("path" -> Json.fromString(r.path.render), "type" -> r.tpe.asJson)))

  /** A kind of node as the palette needs it, for a configuration that is empty. */
  def catalogEntry(d: NodeDescriptor): Json = {
    val none = Json.obj()
    Json.obj(
      "kind" -> d.kind.asJson,
      "category" -> d.category.asJson,
      "label" -> d.label.asJson,
      "description" -> d.description.asJson,
      "config" -> d.config.asJson,
      "sideEffects" -> d.sideEffects.asJson,
      "ports" -> ports(d.ports(none)),
      "inputs" -> d.inputs(none).asJson,
      "role" -> role(d, none).asJson,
      "defaultOutputs" -> d.defaultOutputs(none).asJson
    )
  }

  /** What the node is in a stream: `source` (an ordinary edge starts it and a stream leaves it), `operator`, `sink` (a stream
    * enters it and an ordinary edge leaves it), or `node` for the ones that are not part of any.
    */
  def role(d: NodeDescriptor, params: Json): String =
    if (!d.isStream(params)) "node"
    else if (d.isSource(params)) "source"
    else if (d.isSink(params)) "sink"
    else "operator"

  /** What a node of this kind with this configuration has: its ports can depend on it. `problems` are what is wrong with it. */
  def describe(d: NodeDescriptor, params: Json): Json =
    Json.obj(
      "ports" -> ports(d.ports(params)),
      "inputs" -> d.inputs(params).asJson,
      "role" -> role(d, params).asJson,
      "defaultOutputs" -> d.defaultOutputs(params).asJson,
      "reads" -> reads(d.reads(params)),
      "problems" -> (d.config.validate(params) ++ d.checkParams(params)).asJson
    )

  /** The instance as the UI reads it: a node that will not run shows as `skipped` and not as `dead`. */
  def instanceStatus(s: InstanceStatus): Json = {
    val nodes = s.nodes.map { case (id, n) =>
      id -> InstanceStatus.nodeEncoder(n).mapObject(o => if (o("status").contains(Json.fromString("dead"))) o.add("status", Json.fromString("skipped")) else o)
    }
    InstanceStatus.encoder(s).mapObject(_.add("nodes", Json.fromFields(nodes)))
  }

  /** Whether a flow is switched on, and each of its schedules (the cron nodes where the execution starts) with the next time it runs. */
  def schedule(flow: FlowDef, enabled: Boolean, now: Long): Json =
    Json.obj(
      "enabled" -> enabled.asJson,
      "schedules" -> Json.fromValues(FlowSchedules.of(flow).map { s =>
        val next = CronSchedule.nextFire(s.expression, s.timezone, now).toOption
        Json.obj(
          "node" -> s.origin.label.stripPrefix("cron:").split("/", 2).last.asJson,
          "expression" -> s.expression.text.asJson,
          "timezone" -> s.timezone.asJson,
          "misfire" -> s.misfire.asJson,
          "nextFireAt" -> next.asJson
        )
      })
    )

  /** An audit entry as an SSE event: `type` is its kind, which is what the UI looks at. */
  def event(e: AuditEntry): Json = e.asJson.mapObject(_.add("type", Json.fromString(e.kind)))

  def flowErrors(errors: List[FlowError]): Json = Json.obj("errors" -> errors.asJson)

  /** Problems that are not about a node, in the shape of a [[FlowError]]. */
  def problems(code: String, messages: List[String]): Json =
    Json.obj("errors" -> Json.fromValues(messages.map(m => Json.obj("code" -> code.asJson, "node" -> Json.Null, "message" -> m.asJson))))

  /** A mock as the UI writes it (`{values?, port?, error?}`) or as the engine does (`{type: "output" | "fail", ...}`). */
  def mock(json: Json): Either[String, Mock] =
    if (json.hcursor.downField("type").succeeded) json.as[Mock].left.map(_.message)
    else
      json.hcursor.get[Option[String]]("error") match {
        case Right(Some(error)) => Right(Mock.Fail(error))
        case Right(None) =>
          (for {
            values <- json.hcursor.getOrElse[Map[String, Json]]("values")(Map.empty)
            port <- json.hcursor.getOrElse[String]("port")(Port.Out)
          } yield Mock.Output(values, port)).left.map(_.message)
        case Left(e) => Left(e.message)
      }

  def status(name: String): Either[String, Status] =
    List[Status](Status.Running, Status.Succeeded, Status.Failed).find(_.name == name).toRight(s"unknown status '$name'")

  /** Milliseconds since the epoch, given as that number or as an ISO instant, which is what the UI sends. */
  def time(text: String): Either[String, Long] =
    if (text.nonEmpty && text.forall(_.isDigit)) Right(text.toLong)
    else scala.util.Try(Instant.parse(text).toEpochMilli).toEither.left.map(_ => s"not a time: '$text'")

  def decode[A: Decoder](json: Json): Either[String, A] = json.as[A].left.map(_.message)
}
