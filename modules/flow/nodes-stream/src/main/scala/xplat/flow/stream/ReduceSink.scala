package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Reduces the whole stream to one value in the state: how many elements there were, the sum, average, minimum or maximum of a
  * field, or (`any`, `all`) whether any or all the values of a field that is true or false are true. That is how what a stream
  * found becomes something an `If` can decide on. It fails if the stream is empty and the value is a minimum, a maximum or an
  * average, which do not exist then; `any` of nothing is false and `all` of nothing is true.
  */
final class ReduceSink[F[_]](implicit F: Temporal[F]) extends StreamSink[F] {

  val descriptor: NodeDescriptor = ReduceSink.descriptor

  def drain(ctx: StreamContext[F], upstream: Stream[F, Elem[F]]): F[Map[String, Json]] =
    ReduceSink.params(ctx.params) match {
      case Left(problem) => F.raiseError(new IllegalArgumentException(problem))
      case Right((op, field)) =>
        Ref.of[F, ReduceSink.Acc](ReduceSink.Acc()).flatMap { ref =>
          upstream
            .evalMap { e =>
              field.fold[Either[String, Option[Json]]](Right(None))(p => p.get(e.value).map(Option(_)).toRight(s"'$p' is not in the element")) match {
                case Left(error) => ctx.fail(e, error)
                case Right(v) =>
                  ReduceSink.step(op, ref, v).flatMap {
                    case Some(error) => ctx.fail(e, error)
                    case None        => e.ack
                  }
              }
            }
            .compile
            .drain >> ref.get.flatMap(acc => F.fromEither(ReduceSink.result(op, acc).leftMap(new IllegalStateException(_)))).map(ctx.result)
        }
    }
}

object ReduceSink {
  val Kind: String = "stream.sink.reduce"

  private val Ops = Set("count", "sum", "avg", "min", "max", "any", "all")

  def params(json: Json): Either[String, (String, Option[StatePath])] =
    for {
      op <- json.hcursor.getOrElse[String]("op")("count").left.map(_.message)
      _ <- Either.cond(Ops(op), (), s"unknown op '$op'")
      path <- json.hcursor.get[Option[String]]("path").left.map(_.message).flatMap(_.filter(_.nonEmpty).traverse(StatePath.parse))
      _ <- Either.cond(op == "count" || path.isDefined, (), s"'$op' needs a 'path'")
    } yield (op, path)

  final case class Acc(count: Long = 0, sum: BigDecimal = 0, min: Option[Json] = None, max: Option[Json] = None, any: Boolean = false, all: Boolean = true)

  private[stream] def step[F[_]](op: String, ref: Ref[F, Acc], value: Option[Json])(implicit F: Temporal[F]): F[Option[String]] =
    ref.modify { acc =>
      val counted = acc.copy(count = acc.count + 1)
      val next: Either[String, Acc] = (op, value) match {
        case ("sum" | "avg", Some(v)) => v.asNumber.flatMap(_.toBigDecimal).toRight(s"not a number: ${v.noSpaces}").map(n => counted.copy(sum = acc.sum + n))
        case ("min", Some(v))         => acc.min.fold[Either[String, Option[Json]]](Right(Some(v)))(m => Expr.compare(v, m).map(c => Some(if (c < 0) v else m))).map(m => counted.copy(min = m))
        case ("any" | "all", Some(v)) => v.asBoolean.toRight(s"not true or false: ${v.noSpaces}").map(b => counted.copy(any = acc.any || b, all = acc.all && b))
        case ("max", Some(v))         => acc.max.fold[Either[String, Option[Json]]](Right(Some(v)))(m => Expr.compare(v, m).map(c => Some(if (c > 0) v else m))).map(m => counted.copy(max = m))
        case _                        => Right(counted)
      }
      next.fold(error => (acc, Some(error)), a => (a, None))
    }

  def result(op: String, acc: Acc): Either[String, Json] = op match {
    case "count" => Right(Json.fromLong(acc.count))
    case "sum"   => Right(Json.fromBigDecimal(acc.sum))
    case "avg"   => if (acc.count == 0) Left("there is no average of no elements") else Right(Json.fromBigDecimal(acc.sum / acc.count))
    case "min"   => acc.min.toRight("there is no minimum of no elements")
    case "max"   => acc.max.toRight("there is no maximum of no elements")
    case "any"   => Right(Json.fromBoolean(acc.any))
    case "all"   => Right(Json.fromBoolean(acc.all))
    case other   => Left(s"unknown op '$other'")
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SinkCategory,
    label = "Reduce",
    description = "Ends a stream by reducing it to one value in the state: a count, sum, average, minimum, maximum, any or all.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("op", DataType.Str, required = false, default = Some(Json.fromString("count")), oneOf = Ops.toList.sorted.map(Json.fromString)),
        ConfigSchema.Field("path", DataType.Str, required = false, description = Some("Field of each element (not for count)"))
      )
    ),
    inputs = _ => StreamPorts.input,
    defaultOutputs = _ => List(OutputDecl("result", StatePath(List("reduced")), DataType.Num)),
    checkParams = p => params(p).left.toOption.toList
  )
}
