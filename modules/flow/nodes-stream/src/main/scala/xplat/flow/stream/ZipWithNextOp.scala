package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Puts each element next to the one after it, like `zipWithNext` of fs2: `{"value": <the element>, "next": <the one after, or null>}`.
  * So each element waits for the next one to come before it leaves. */
final class ZipWithNextOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = ZipWithNextOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    in.zipWithNext.map { case (e, after) => ctx.element(e, Json.obj("value" -> e.value, "next" -> after.fold(Json.Null)(_.value))) }
  }
}

object ZipWithNextOp {
  val Kind: String = "stream.zipWithNext"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Zip with next",
    description = "Puts each element next to the one after it: {value, next}, next null for the last (fs2 zipWithNext).",
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = _ => Nil
  )
}
