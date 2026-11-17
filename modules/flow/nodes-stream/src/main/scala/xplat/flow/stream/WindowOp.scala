package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

import scala.concurrent.duration._

/** Groups the elements into windows. Each window is one element, `{"items": [...], "count": n, "start": ms, "end": ms}` (`start` and
  * `end` are when the first and the last of its elements came in).
  *
  *   - `tumbling` windows follow one another without sharing elements: of `size` elements (`by: count`) or of what comes in during
  *     `duration` (`by: time`). The last one, when the stream ends, may be smaller. A window is acknowledged when it is passed on.
  *   - `sliding` windows overlap: every `every` elements (or every `every` of time) there is a window of the last `size` elements (or of
  *     the last `duration`). A window in memory is lost if the server stops, so elements are acknowledged as they come in.
  *
  * In a simulation, time windows are one single window with everything the finite stream gave.
  */
final class WindowOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = WindowOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    WindowOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        val in = inputs.getOrElse(Port.In, Stream.empty)
        p match {
          case WindowOp.Tumbling(WindowOp.Count(n))  => in.chunkN(n, allowFewer = true).evalMap(c => window(c.toList, combine = true))
          case WindowOp.Tumbling(WindowOp.Time(d)) =>
            in.groupWithin(Int.MaxValue, if (ctx.simulated) 365.days else d).evalMap(c => window(c.toList, combine = true))
          case WindowOp.Sliding(WindowOp.Count(n), WindowOp.Count(every)) =>
            in.evalTap(_.ack).sliding(n).zipWithIndex.collect { case (c, i) if i % every == 0 => c }.evalMap(c => window(c.toList, combine = false))
          case WindowOp.Sliding(WindowOp.Count(_), _) => Stream.raiseError[F](new IllegalArgumentException("a sliding window by count moves every number of elements"))
          case WindowOp.Sliding(WindowOp.Time(d), WindowOp.Time(every)) =>
            if (ctx.simulated) in.chunkAll.evalMap(c => window(c.toList, combine = true))
            else slidingByTime(in, d, every)
          case WindowOp.Sliding(WindowOp.Time(_), _) => Stream.raiseError[F](new IllegalArgumentException("a sliding window by time moves every length of time"))
        }
    }

  private def window(elements: List[Elem[F]], combine: Boolean): F[Elem[F]] =
    F.pure {
      val items = elements.map(_.value)
      val first = elements.map(_.at).minOption.getOrElse(0L)
      val last = elements.map(_.at).maxOption.getOrElse(0L)
      Elem(
        Json.obj("items" -> Json.fromValues(items), "count" -> Json.fromInt(items.size), "start" -> Json.fromLong(first), "end" -> Json.fromLong(last)),
        first,
        if (combine) elements.traverse_(_.ack) else F.unit
      )
    }

  /** Every `every`, the elements that came in during the last `size`. The stream ends with a last window of what is left. */
  private def slidingByTime(in: Stream[F, Elem[F]], size: FiniteDuration, every: FiniteDuration): Stream[F, Elem[F]] =
    Stream.eval(Ref.of[F, Vector[Elem[F]]](Vector.empty)).flatMap { buffer =>
      def current: F[Elem[F]] =
        F.realTime.map(_.toMillis).flatMap { now =>
          buffer.updateAndGet(_.filter(_.at > now - size.toMillis)).flatMap(es => window(es.toList, combine = false))
        }
      val filling = in.evalTap(_.ack).evalMap(e => buffer.update(_ :+ e)).drain
      val ticks = Stream.awakeEvery[F](every).evalMap(_ => current)
      filling.mergeHaltL(ticks) ++ Stream.eval(current)
    }
}

object WindowOp {
  val Kind: String = "stream.window"

  /** The size of a window or the step of a sliding one: a number of elements, or a length of time. */
  sealed trait Measure
  final case class Count(n: Int) extends Measure
  final case class Time(d: FiniteDuration) extends Measure

  sealed trait Shape
  final case class Tumbling(size: Measure) extends Shape
  final case class Sliding(size: Measure, every: Measure) extends Shape

  def params(json: Json): Either[String, Shape] = {
    val c = json.hcursor
    for {
      kind <- c.getOrElse[String]("kind")("tumbling").left.map(_.message)
      by <- c.getOrElse[String]("by")("count").left.map(_.message)
      shape <- (kind, by) match {
        case ("tumbling", "count") => size(c).map(n => Tumbling(Count(n)))
        case ("tumbling", "time")  => duration(json, "duration").map(d => Tumbling(Time(d)))
        case ("sliding", "count") =>
          for {
            n <- size(c)
            every <- c.getOrElse[Int]("every")(1).left.map(_.message)
            _ <- Either.cond(every > 0, (), "every must be more than 0")
          } yield Sliding(Count(n), Count(every))
        case ("sliding", "time") =>
          for {
            d <- duration(json, "duration")
            every <- duration(json, "every")
          } yield Sliding(Time(d), Time(every))
        case _ => Left(s"unknown window: kind '$kind' by '$by'")
      }
    } yield shape
  }

  private def size(c: io.circe.HCursor): Either[String, Int] =
    c.get[Int]("size").left.map(_ => "missing field 'size'").flatMap(n => Either.cond(n > 0, n, "size must be more than 0"))

  private def duration(json: Json, field: String): Either[String, FiniteDuration] =
    json.hcursor.get[String](field).left.map(_ => s"missing field '$field'").flatMap(StreamPorts.duration)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Window",
    description = "Groups the elements into windows, by count or by time; each window is one element with its items.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("kind", DataType.Str, required = false, default = Some(Json.fromString("tumbling")), oneOf = List(Json.fromString("tumbling"), Json.fromString("sliding"))),
        ConfigSchema.Field("by", DataType.Str, required = false, default = Some(Json.fromString("count")), oneOf = List(Json.fromString("count"), Json.fromString("time"))),
        ConfigSchema.Field("size", DataType.Num, required = false, description = Some("Elements in a window (by count)")),
        ConfigSchema.Field("duration", DataType.Str, required = false, description = Some("Length of a window (by time), such as '10 seconds'")),
        ConfigSchema.Field("every", DataType.AnyJson, required = false, description = Some("Sliding: the step, in elements (by count) or time (by time)"))
      )
    ),
    ports = _ => StreamPorts.output(DataType.Obj(Map("items" -> DataType.ListOf(DataType.AnyJson), "count" -> DataType.Num))),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )

}
