package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Puts each element next to the one before it, like `zipWithPrevious` of fs2: `{"previous": <the one before, or null>, "value": <the element>}`. */
final class ZipWithPreviousOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = ZipWithPreviousOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    in.zipWithPrevious.map { case (before, e) => ctx.element(e, Json.obj("previous" -> before.fold(Json.Null)(_.value), "value" -> e.value)) }
  }
}

object ZipWithPreviousOp {
  val Kind: String = "stream.zipWithPrevious"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Zip with previous",
    description = "Puts each element next to the one before it: {previous, value}, previous null for the first (fs2 zipWithPrevious).",
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = _ => Nil
  )
}
