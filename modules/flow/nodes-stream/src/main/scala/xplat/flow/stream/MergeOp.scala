package xplat.flow.stream

import cats.effect.Temporal
import fs2.Stream
import xplat.flow.model._

/** Puts into one stream the elements of every edge that enters it, as they come, in no particular order. */
final class MergeOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = MergeOp.descriptor

  // The segment has already merged the edges that enter `in`.
  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = inputs.getOrElse(Port.In, Stream.empty)
}

object MergeOp {
  val Kind: String = "stream.merge"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Merge",
    description = "Puts the elements of every stream that enters it into one, as they come.",
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input
  )
}
