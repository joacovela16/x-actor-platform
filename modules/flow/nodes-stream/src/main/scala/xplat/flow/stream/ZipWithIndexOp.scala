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
    // The next index is its checkpoint: after a restart the numbers go on.
    val start = ctx.restored.flatMap(_.asNumber).flatMap(_.toLong).getOrElse(0L)
    Stream.eval(Ref.of[F, Long](start)).flatMap { next =>
      Stream.exec(ctx.keep(next.get.map(Json.fromLong))) ++
        in.evalMap(e => next.getAndUpdate(_ + 1).map(i => ctx.element(e, Json.obj("value" -> e.value, "index" -> Json.fromLong(i)))))
    }
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
