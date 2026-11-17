package xplat.flow.stream

import cats.effect.Temporal
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Pairs the elements of two streams in order: the first with the first, the second with the second. It ends with the shorter one.
  * The element is `{"left": ..., "right": ...}`.
  */
final class ZipOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = ZipOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    inputs.getOrElse("left", Stream.empty).zip(inputs.getOrElse("right", Stream.empty)).map { case (l, r) =>
      Elem(Json.obj("left" -> l.value, "right" -> r.value), math.min(l.at, r.at), l.ack *> r.ack)
    }
}

object ZipOp {
  val Kind: String = "stream.zip"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Zip",
    description = "Pairs the elements of two streams in order, as {left, right}. It ends with the shorter one.",
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.sides
  )
}
