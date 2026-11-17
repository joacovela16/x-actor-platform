package xplat.flow.stream

import cats.effect.Temporal
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.engine.CronSchedule
import xplat.flow.model._

/** One element each time a cron expression matches: `{"at": <milliseconds since the epoch>}`. Without `maxTicks` it never ends, so
  * the instance runs until it is cancelled. In a simulation it gives `maxTicks` (3 if it has none) at the times it would have, with no
  * waiting.
  */
final class CronSource[F[_]](implicit F: Temporal[F]) extends StreamSource[F] {

  val descriptor: NodeDescriptor = CronSource.descriptor

  def open(ctx: StreamContext[F]): Stream[F, Elem[F]] =
    CronSource.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        val limit: Option[Long] = if (ctx.simulated) Some(p.maxTicks.getOrElse(3L)) else p.maxTicks
        val ticks = Stream.eval(ctx.now).flatMap { start =>
          Stream.unfoldEval[F, Long, Long](start) { after =>
            CronSchedule.nextFire(p.expression, p.timezone, after) match {
              case Left(problem) => F.raiseError[Option[(Long, Long)]](new IllegalArgumentException(problem))
              case Right(at)     => (if (ctx.simulated) F.unit else ctx.now.flatMap(n => F.sleep(scala.concurrent.duration.FiniteDuration(math.max(0L, at - n), scala.concurrent.duration.MILLISECONDS)))).as(Option((at, at)))
            }
          }
        }
        limit.fold(ticks)(n => ticks.take(n)).map(at => Elem.of[F](Json.obj("at" -> Json.fromLong(at)), at))
    }
}

object CronSource {
  val Kind: String = "stream.source.cron"

  final case class Params(expression: CronExpression, timezone: String, maxTicks: Option[Long])

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      text <- c.get[String]("expression").left.map(_ => "missing field 'expression'")
      expression <- CronExpression.parse(text).left.map(e => s"expression: $e")
      zone <- c.getOrElse[String]("timezone")("UTC").left.map(e => e.message)
      _ <- CronSchedule.zone(zone)
      max <- c.get[Option[Long]]("maxTicks").left.map(e => s"maxTicks: ${e.message}")
    } yield Params(expression, zone, max)
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SourceCategory,
    label = "Cron ticks",
    description = "Starts a stream that gives one element each time a cron expression matches. Without a maximum it never ends.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("expression", DataType.Str, description = Some("Cron expression, 5 or 6 fields")),
        ConfigSchema.Field("timezone", DataType.Str, required = false, default = Some(Json.fromString("UTC"))),
        ConfigSchema.Field("maxTicks", DataType.Num, required = false, description = Some("Stop after this many ticks; without it, never"))
      )
    ),
    ports = _ => StreamPorts.output(DataType.Obj(Map("at" -> DataType.Num))),
    checkParams = p => params(p).left.toOption.toList
  )
}
