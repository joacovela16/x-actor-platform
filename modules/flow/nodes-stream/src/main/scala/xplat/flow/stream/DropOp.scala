package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Leaves out the first `n` elements and passes on the rest, like `drop` of fs2. The ones it leaves out count as dropped. */
final class DropOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = DropOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    StreamPorts.count(ctx.params, "n") match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(n) =>
        in.zipWithIndex.evalMapFilter { case (e, i) => if (i < n) ctx.drop(e).as(Option.empty[Elem[F]]) else F.pure(Option(e)) }
    }
  }
}

object DropOp {
  val Kind: String = "stream.drop"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Drop",
    description = "Leaves out the first n elements and passes on the rest (fs2 drop).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("n", DataType.Num, description = Some("How many elements to leave out"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => StreamPorts.count(p, "n").left.toOption.toList
  )
}
