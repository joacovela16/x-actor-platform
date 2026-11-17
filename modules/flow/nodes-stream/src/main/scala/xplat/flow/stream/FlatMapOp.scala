package xplat.flow.stream

import cats.effect.Temporal
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Turns an element that has a list into one element for each item of it. With `merge`, an item that is an object keeps the other
  * fields of the element. An element without the list, or where it is not a list, is a dead letter; one with an empty list gives
  * nothing.
  */
final class FlatMapOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = FlatMapOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    FlatMapOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right((path, merge)) =>
        inputs.getOrElse(Port.In, Stream.empty).flatMap { e =>
          path.get(e.value).flatMap(_.asArray) match {
            case None => Stream.exec(ctx.fail(e, s"'$path' is not a list in the element"))
            case Some(items) if items.isEmpty => Stream.exec(ctx.drop(e))
            case Some(items) =>
              val rest = e.value.mapObject(_.remove(path.segments.head))
              val values = items.map(item => if (merge && item.isObject) rest.deepMerge(item) else item)
              val last = values.size - 1
              // The element is done when its last item is: only that one carries its acknowledgement.
              Stream.emits(values.zipWithIndex.map { case (v, i) => e.copy(value = v, ack = if (i == last) e.ack else F.unit) })
          }
        }
    }
}

object FlatMapOp {
  val Kind: String = "stream.flatMap"

  def params(json: Json): Either[String, (StatePath, Boolean)] =
    for {
      text <- json.hcursor.get[String]("path").left.map(_ => "missing field 'path'")
      path <- StatePath.parse(text)
      merge <- json.hcursor.getOrElse[Boolean]("merge")(false).left.map(_.message)
    } yield (path, merge)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Flat map",
    description = "Turns an element that has a list into one element for each item of the list. 'path' is where the list is inside the element.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("path", DataType.Str, description = Some("Path of the list inside the element")),
        ConfigSchema.Field("merge", DataType.Bool, required = false, default = Some(Json.False), description = Some("Items that are objects keep the other fields of the element"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )
}
