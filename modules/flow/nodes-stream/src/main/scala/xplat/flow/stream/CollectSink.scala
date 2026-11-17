package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Collects the elements into a list in the state, up to `limit` (the ones after that are dropped, and counted as dropped). With a
  * `path`, it collects that field of each element. It writes the list to the first output it declares.
  */
final class CollectSink[F[_]](implicit F: Temporal[F]) extends StreamSink[F] {

  val descriptor: NodeDescriptor = CollectSink.descriptor

  def drain(ctx: StreamContext[F], upstream: Stream[F, Elem[F]]): F[Map[String, Json]] =
    CollectSink.params(ctx.params) match {
      case Left(problem) => F.raiseError(new IllegalArgumentException(problem))
      case Right((limit, field)) =>
        Ref.of[F, Vector[Json]](Vector.empty).flatMap { ref =>
          upstream
            .evalMap { e =>
              field.fold[Either[String, Json]](Right(e.value))(p => p.get(e.value).toRight(s"'$p' is not in the element")) match {
                case Left(error) => ctx.fail(e, error)
                case Right(v) =>
                  ref.modify(c => if (c.size < limit) (c :+ v, true) else (c, false)).flatMap(kept => if (kept) e.ack else ctx.drop(e))
              }
            }
            .compile
            .drain >> ref.get.map(c => ctx.result(Json.fromValues(c)))
        }
    }
}

object CollectSink {
  val Kind: String = "stream.sink.collect"

  def params(json: Json): Either[String, (Int, Option[StatePath])] =
    for {
      limit <- json.hcursor.getOrElse[Int]("limit")(1000).left.map(_.message)
      _ <- Either.cond(limit > 0, (), "limit must be more than 0")
      path <- json.hcursor.get[Option[String]]("path").left.map(_.message).flatMap(_.filter(_.nonEmpty).traverse(StatePath.parse))
    } yield (limit, path)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SinkCategory,
    label = "Collect",
    description = "Ends a stream by collecting its elements (or one field of each) into a list in the state.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("limit", DataType.Num, required = false, default = Some(Json.fromInt(1000)), description = Some("Most elements to keep")),
        ConfigSchema.Field("path", DataType.Str, required = false, description = Some("Collect this field of each element; the whole element if empty"))
      )
    ),
    inputs = _ => StreamPorts.input,
    defaultOutputs = _ => List(OutputDecl("result", StatePath(List("collected")), DataType.ListOf(DataType.AnyJson))),
    checkParams = p => params(p).left.toOption.toList
  )
}
