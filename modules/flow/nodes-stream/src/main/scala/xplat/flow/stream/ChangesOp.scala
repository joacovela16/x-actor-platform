package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Passes an element on only when its key is different from that of the element just before it, like `changesBy` of fs2: a run of
  * equal keys gives its first element. Unlike [[DedupeOp]] it remembers only the last key. The key is a field of the element, or the
  * whole element; an element without it is a dead letter. */
final class ChangesOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = ChangesOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    StreamPorts.key(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(key) =>
        Stream.eval(Ref.of[F, Option[Json]](ctx.restored.flatMap(_.hcursor.downField("last").focus))).flatMap { last =>
          Stream.exec(ctx.keep(last.get.map(l => Json.fromFields(l.map("last" -> _).toList)))) ++ in.evalMapFilter { e =>
            StreamPorts.keyOf(key, e.value) match {
              case Left(error) => ctx.fail(e, error).as(Option.empty[Elem[F]])
              case Right(k)    => last.getAndSet(Some(k)).flatMap(before => if (before.contains(k)) ctx.drop(e).as(Option.empty[Elem[F]]) else F.pure(Option(e)))
            }
          }
        }
    }
  }
}

object ChangesOp {
  val Kind: String = "stream.changes"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Changes",
    description = "Passes an element on only when its key is different from the one of the element before it (fs2 changesBy).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("key", DataType.Str, required = false, description = Some("Path of the key in the element; the whole element if empty"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => StreamPorts.key(p).left.toOption.toList
  )
}
