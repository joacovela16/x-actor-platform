package xplat.flow.stream

import cats.effect.Temporal
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** The elements of a list that is in the state: each item of the array is one element. It is read when the segment starts. */
final class ListSource[F[_]](implicit F: Temporal[F]) extends StreamSource[F] {

  val descriptor: NodeDescriptor = ListSource.descriptor

  def open(ctx: StreamContext[F]): Stream[F, Elem[F]] =
    ListSource.path(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(path) =>
        path.get(ctx.input.state).flatMap(_.asArray) match {
          case None        => Stream.raiseError[F](new IllegalArgumentException(s"'$path' is not a list in the state"))
          case Some(items) => Stream.eval(ctx.now).flatMap(at => Stream.emits(items.map(Elem.of[F](_, at))))
        }
    }
}

object ListSource {
  val Kind: String = "stream.source.list"

  def path(params: Json): Either[String, StatePath] =
    StreamPorts.text(params, "path").toRight("missing field 'path'").flatMap(StreamPorts.path)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SourceCategory,
    label = "List",
    description = "Starts a stream from a list in the state: each item of the list is one element. Use it to stream what an ordinary node (a query, a find...) left in the state.",
    config = ConfigSchema(List(ConfigSchema.Field("path", DataType.Str, description = Some("Path in the state of a list: each item is an element")))),
    ports = _ => StreamPorts.output(),
    reads = p => path(p).toOption.toList.map(StateRead(_, DataType.ListOf(DataType.AnyJson))),
    checkParams = p => path(p).left.toOption.toList
  )
}
