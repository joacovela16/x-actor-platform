package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Groups the elements that come one after another with the same key, like `groupAdjacentBy` of fs2: each group is one element,
  * `{"key": <the key>, "items": [...], "count": n}`, which leaves when an element with another key comes (or the stream ends). A
  * group is acknowledged when it is. The key is a field of the element, or the whole element; an element without it is a dead letter. */
final class GroupAdjacentByOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = GroupAdjacentByOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    StreamPorts.key(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(key) =>
        in.evalMapFilter(e => StreamPorts.keyOf(key, e.value) match {
            case Left(error) => ctx.fail(e, error).as(Option.empty[(Json, Elem[F])])
            case Right(k)    => F.pure(Option(k -> e))
          })
          .groupAdjacentBy(_._1)
          .map { case (k, chunk) =>
            val es = chunk.toList.map(_._2)
            Elem[F](
              Json.obj("key" -> k, "items" -> Json.fromValues(es.map(_.value)), "count" -> Json.fromInt(es.size)),
              es.map(_.at).min,
              es.traverse_(_.ack)
            )
          }
    }
  }
}

object GroupAdjacentByOp {
  val Kind: String = "stream.groupAdjacentBy"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Group adjacent by",
    description = "Groups the elements that come one after another with the same key: each group is {key, items, count} (fs2 groupAdjacentBy).",
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
