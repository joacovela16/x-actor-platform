package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Numbers the elements from 0, like `zipWithIndex` of fs2: each one becomes `{"value": <the element>, "index": n}`. */
final class ZipWithIndexOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = ZipWithIndexOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    in.zipWithIndex.map { case (e, i) => ctx.element(e, Json.obj("value" -> e.value, "index" -> Json.fromLong(i))) }
  }
}

object ZipWithIndexOp {
  val Kind: String = "stream.zipWithIndex"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Zip with index",
    description = "Numbers the elements from 0: each one becomes {value, index} (fs2 zipWithIndex).",
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = _ => Nil
  )
}
