package xplat.flow.stream

import cats.effect.Temporal
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** The whole numbers from `from` up to, not including, `to`, in steps of `step`. Useful to repeat something and to try a stream. */
final class RangeSource[F[_]](implicit F: Temporal[F]) extends StreamSource[F] {

  val descriptor: NodeDescriptor = RangeSource.descriptor

  def open(ctx: StreamContext[F]): Stream[F, Elem[F]] =
    RangeSource.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right((from, to, step)) => Stream.eval(ctx.now).flatMap(at => Stream.range(from, to, step).map(n => Elem.of[F](Json.fromLong(n), at)))
    }
}

object RangeSource {
  val Kind: String = "stream.source.range"

  def params(json: Json): Either[String, (Long, Long, Long)] = {
    val c = json.hcursor
    for {
      from <- c.getOrElse[Long]("from")(0L).left.map(e => s"from: ${e.message}")
      to <- c.get[Long]("to").left.map(e => s"to: ${e.message}")
      step <- c.getOrElse[Long]("step")(1L).left.map(e => s"step: ${e.message}")
      _ <- Either.cond(step > 0, (), "step must be more than 0")
    } yield (from, to, step)
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SourceCategory,
    label = "Range",
    description = "Starts a stream of the whole numbers from 'from' up to, not including, 'to'.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("from", DataType.Num, required = false, default = Some(Json.fromInt(0))),
        ConfigSchema.Field("to", DataType.Num, description = Some("Up to, not including")),
        ConfigSchema.Field("step", DataType.Num, required = false, default = Some(Json.fromInt(1)))
      )
    ),
    ports = _ => StreamPorts.output(DataType.Num),
    checkParams = p => params(p).left.toOption.toList
  )
}
