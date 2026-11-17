package xplat.flow.stream

import cats.effect.Temporal
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

import scala.concurrent.duration._

/** Lets at most `elements` through every `per`, one after another at an even pace. The elements that wait hold back the nodes before
  * it (backpressure). In a simulation it does not wait.
  */
final class ThrottleOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = ThrottleOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    ThrottleOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(gap) =>
        val in = inputs.getOrElse(Port.In, Stream.empty)
        // The first goes at once and each of the others waits `gap` after the one before; there is no wait after the last.
        if (ctx.simulated) in else in.zipWithIndex.evalMap { case (e, i) => (if (i == 0L) F.unit else F.sleep(gap)).as(e) }
    }
}

object ThrottleOp {
  val Kind: String = "stream.throttle"

  /** The time between two elements. */
  def params(json: Json): Either[String, FiniteDuration] =
    for {
      n <- json.hcursor.getOrElse[Int]("elements")(1).left.map(_.message)
      _ <- Either.cond(n > 0, (), "elements must be more than 0")
      per <- StreamPorts.duration(json.hcursor.getOrElse[String]("per")("1 second").getOrElse(""))
    } yield FiniteDuration(math.max(1L, per.toNanos / n), NANOSECONDS)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Throttle",
    description = "Lets at most a number of elements through in each period, at an even pace.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("elements", DataType.Num, required = false, default = Some(Json.fromInt(1))),
        ConfigSchema.Field("per", DataType.Str, required = false, default = Some(Json.fromString("1 second")), description = Some("A length of time such as '1 second'"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )
}
