package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Keeps the first `n` elements and then ends, like `take` of fs2. The nodes before it, up to the sources, are stopped then: a
  * `take` after a queue or a cron ends what never would. What was on its way when it ended is acknowledged, as dropped. */
final class TakeOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = TakeOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    StreamPorts.count(ctx.params, "n") match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(n) =>
        in.take(n.toLong)
    }
  }
}

object TakeOp {
  val Kind: String = "stream.take"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Take",
    description = "Keeps the first n elements and ends the stream, closing the sources before it (fs2 take).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("n", DataType.Num, description = Some("How many elements to keep"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => StreamPorts.count(p, "n").left.toOption.toList
  )
}
