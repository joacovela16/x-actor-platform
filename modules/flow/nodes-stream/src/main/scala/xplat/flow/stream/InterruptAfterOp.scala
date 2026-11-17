package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Ends the stream once `duration` has passed since the segment started, like `interruptAfter` of fs2, and stops the nodes before it.
  * In a simulation it does not wait: the finite stream passes whole. */
final class InterruptAfterOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = InterruptAfterOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    StreamPorts.text(ctx.params, "duration").toRight("missing field 'duration'").flatMap(StreamPorts.duration) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(d) =>
        if (ctx.simulated) in else in.interruptAfter(d)
    }
  }
}

object InterruptAfterOp {
  val Kind: String = "stream.interruptAfter"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Interrupt after",
    description = "Ends the stream after a length of time, closing the sources before it (fs2 interruptAfter).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("duration", DataType.Str, description = Some("How long, such as 30 seconds or 5 minutes"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => StreamPorts.text(p, "duration").toRight("missing field 'duration'").flatMap(StreamPorts.duration).left.toOption.toList
  )
}
