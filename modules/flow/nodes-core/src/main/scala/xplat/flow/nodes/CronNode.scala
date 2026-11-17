package xplat.flow.nodes

import cats.Applicative
import cats.syntax.applicative._
import io.circe.{Decoder, Json}
import xplat.flow.model._

/** The node of time. It has two modes, which the graph decides and not a setting:
  *
  *   - **Launcher** (no incoming edge): the flow is started by the schedule. A scheduler (see `CronScheduler` in the engine) starts an
  *     instance of the flow at each time the expression matches, with the node's `inputs` as the inputs of the flow. The node
  *     itself, when it runs inside that instance, does nothing and lets the execution go on.
  *   - **Timer** (with incoming edges): the execution waits here until the next time the expression matches, and then goes on
  *     through `out`. The wait is recorded, so it survives a restart: nothing runs again when the time comes.
  *
  * @param next
  *   computes the next time an expression matches after a time, in a time zone. It is given by the runtime (the engine uses
  *   `java.time`), so this class does not depend on a calendar and can be used in the browser.
  */
final class CronNode[F[_]: Applicative](next: CronNode.NextFire) extends NodeType[F] {

  val descriptor: NodeDescriptor = CronNode.descriptor

  def run(input: NodeInput): F[NodeOutput] = CronNode.evaluate(next, input).pure[F]
}

object CronNode {

  val Kind: String = "core.cron"

  /** The next time, in milliseconds since the epoch, that the expression matches strictly after `after` (also in milliseconds), in
    * the time zone with this id; or why it cannot be known.
    */
  type NextFire = (CronExpression, String, Long) => Either[String, Long]

  /** For a runtime that has no calendar: a timer fails and a launcher is not scheduled. */
  val NoClock: NextFire = (_, _, _) => Left("this runtime cannot compute the next time of a cron expression")

  val FireOnce: String = "fire-once"
  val Skip: String = "skip"

  /** @param expression
    *   see [[CronExpression]].
    * @param timezone
    *   IANA id, such as `Europe/Madrid`.
    * @param misfire
    *   launcher: what to do with the times that passed while the server was stopped. `fire-once` starts one instance for them (the
    *   latest), `skip` starts none.
    * @param maxWait
    *   timer: it fails through `error` if the next time is farther than this (`90s`, `30m`, `2h`, `1d`).
    * @param inputs
    *   launcher: the inputs of the flow for the instances that the schedule starts, by name of the input.
    */
  final case class Params(
      expression: String,
      timezone: String = "UTC",
      misfire: String = FireOnce,
      maxWait: Option[String] = None,
      inputs: Json = Json.obj()
  ) {
    def parsed: Either[String, CronExpression] = CronExpression.parse(expression)
    def maxWaitMillis: Either[String, Option[Long]] = maxWait match {
      case None    => Right(None)
      case Some(t) => duration(t).map(Some(_))
    }
  }

  object Params {
    implicit val decoder: Decoder[Params] = Decoder.instance { c =>
      for {
        expression <- c.get[String]("expression")
        timezone <- c.getOrElse[String]("timezone")("UTC")
        misfire <- c.getOrElse[String]("misfire")(FireOnce)
        maxWait <- c.get[Option[String]]("maxWait")
        inputs <- c.getOrElse[Json]("inputs")(Json.obj())
      } yield Params(expression, timezone, misfire, maxWait, if (inputs.isNull) Json.obj() else inputs)
    }
  }

  /** `90s`, `30m`, `2h` or `1d`, in milliseconds. */
  def duration(text: String): Either[String, Long] = {
    val t = text.trim
    val unit = t.lastOption.flatMap(u => Map('s' -> 1000L, 'm' -> 60000L, 'h' -> 3600000L, 'd' -> 86400000L).get(u))
    val amount = t.dropRight(1)
    (unit, amount.nonEmpty && amount.length <= 9 && amount.forall(_.isDigit)) match {
      case (Some(u), true) => Right(amount.toLong * u)
      case _               => Left(s"'$text' is not a duration; use a number and s, m, h or d, such as 30m")
    }
  }

  /** What the zone id can look like. Whether it exists is known by whoever has the calendar: the timer fails if it does not. */
  private val ZoneId = "^(UTC|GMT|Z|[A-Za-z][A-Za-z0-9_+-]*(/[A-Za-z0-9_+-]+)*)$".r

  def params(json: Json): Either[String, Params] = json.as[Params].left.map(e => s"invalid configuration: ${e.message}")

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Triggers",
    label = "Cron",
    description = "Without incoming edges it starts the flow on a schedule; with incoming edges it waits until the next time the expression matches.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("expression", DataType.Str, description = Some("Cron expression: 5 fields (minute to day of week) or 6 with seconds first; also @hourly, @daily...")),
        ConfigSchema.Field("timezone", DataType.Str, required = false, default = Some(Json.fromString("UTC")), description = Some("IANA time zone, such as Europe/Madrid")),
        ConfigSchema.Field(
          "misfire",
          DataType.Str,
          required = false,
          default = Some(Json.fromString(FireOnce)),
          oneOf = List(Json.fromString(FireOnce), Json.fromString(Skip)),
          description = Some("Launcher: what to do with the times missed while the server was stopped")
        ),
        ConfigSchema.Field("maxWait", DataType.Str, required = false, description = Some("Timer: fail if the next time is farther than this, such as 30m or 1d")),
        ConfigSchema.Field("inputs", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Launcher: inputs of the flow for the instances that the schedule starts"))
      )
    ),
    sideEffects = SideEffects.Pure,
    checkParams = json =>
      params(json) match {
        case Left(problem) => List(problem)
        case Right(p) =>
          p.parsed.left.toOption.map(e => s"expression: $e").toList ++
            (if (ZoneId.findFirstIn(p.timezone).isDefined) Nil else List(s"timezone: '${p.timezone}' is not a time zone id")) ++
            p.maxWaitMillis.left.toOption.map(e => s"maxWait: $e").toList ++
            (if (p.inputs.isObject) Nil else List("inputs must be an object"))
      }
  )

  /** What the node answers: nothing at the start of a flow, a wait anywhere else. */
  def evaluate(next: NextFire, input: NodeInput): NodeOutput =
    params(input.params).flatMap(p => p.parsed.map(p -> _)) match {
      case Left(problem) => NodeOutput.failed(problem)
      case Right((_, _)) if input.entry => NodeOutput.ok()
      case Right((p, expression)) =>
        p.maxWaitMillis match {
          case Left(problem) => NodeOutput.failed(problem)
          case Right(limit) =>
            next(expression, p.timezone, input.now) match {
              case Left(problem) => NodeOutput.failed(problem)
              case Right(fireAt) if limit.exists(fireAt - input.now > _) =>
                NodeOutput.failed(s"the next time is ${(fireAt - input.now) / 1000}s away, beyond maxWait ${p.maxWait.getOrElse("")}")
              case Right(fireAt) =>
                NodeOutput.waitUntil(
                  fireAt,
                  Map("fireAt" -> Json.fromLong(fireAt), "expression" -> Json.fromString(p.expression), "timezone" -> Json.fromString(p.timezone))
                )
            }
        }
    }
}
