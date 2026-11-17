package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Drops an element whose key was already seen. The key is a field of the element, or the whole element. It remembers the last
  * `max` keys, so memory does not grow for ever; a key that was forgotten can pass again. Only in memory: after a restart it
  * starts empty.
  */
final class DedupeOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = DedupeOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    DedupeOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right((key, max)) =>
        // The keys it remembers are its checkpoint: after a restart a message that comes again is still a duplicate.
        val initial = ctx.restored.flatMap(_.as[Vector[String]].toOption).getOrElse(Vector.empty).takeRight(max)
        Stream.eval(Ref.of[F, (Set[String], Vector[String])]((initial.toSet, initial))).flatMap { seen =>
          Stream.exec(ctx.keep(seen.get.map(s => Json.fromValues(s._2.map(Json.fromString))))) ++ inputs.getOrElse(Port.In, Stream.empty).evalFilter { e =>
            val id = key.fold(Option(e.value.noSpaces))(_.get(e.value).map(_.noSpaces))
            id match {
              case None => ctx.fail(e, s"'${key.get}' is not in the element").as(false)
              case Some(k) =>
                seen
                  .modify { case (set, order) =>
                    if (set(k)) ((set, order), false)
                    else {
                      val all = order :+ k
                      val (kept, forgotten) = if (all.size > max) (all.drop(all.size - max), all.take(all.size - max)) else (all, Vector.empty[String])
                      ((set -- forgotten + k, kept), true)
                    }
                  }
                  .flatMap(fresh => if (fresh) F.pure(true) else ctx.drop(e).as(false))
            }
          }
        }
    }
}

object DedupeOp {
  val Kind: String = "stream.dedupe"

  def params(json: Json): Either[String, (Option[StatePath], Int)] =
    for {
      key <- json.hcursor.get[Option[String]]("key").left.map(_.message).flatMap(_.filter(_.nonEmpty).traverse(StatePath.parse))
      max <- json.hcursor.getOrElse[Int]("max")(10000).left.map(_.message)
      _ <- Either.cond(max > 0, (), "max must be more than 0")
    } yield (key, max)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Deduplicate",
    description = "Drops an element whose key was already seen recently.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("key", DataType.Str, required = false, description = Some("Path of the key in the element; the whole element if empty")),
        ConfigSchema.Field("max", DataType.Num, required = false, default = Some(Json.fromInt(10000)), description = Some("How many keys to remember"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )
}
