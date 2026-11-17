package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Joins two streams by key: when an element of one side has the same key as elements of the other that were already seen, it gives
  * one `{"left": ..., "right": ...}` for each pair, in the order they meet. Both sides are kept in memory, the last `maxBuffer` keys
  * of each, so it fits when the matches come close in time. An element without its key is a dead letter. Elements are acknowledged
  * as they are taken in, because what is kept is only in memory anyway.
  */
final class JoinOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = JoinOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    JoinOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        Stream.eval(Ref.of[F, JoinOp.Buffers](JoinOp.Buffers())).flatMap { buffers =>
          val left = inputs.getOrElse("left", Stream.empty).map(e => (true, e))
          val right = inputs.getOrElse("right", Stream.empty).map(e => (false, e))
          left.merge(right).flatMap { case (isLeft, e) =>
            (if (isLeft) p.leftKey else p.rightKey).get(e.value) match {
              case None => Stream.exec(ctx.fail(e, s"'${if (isLeft) p.leftKey else p.rightKey}' is not in the element"))
              case Some(key) =>
                Stream.eval(buffers.modify(_.add(isLeft, key.noSpaces, e.value, p.maxBuffer)) <* e.ack).flatMap { matches =>
                  Stream.emits(matches.map { case (l, r) => Elem[F](Json.obj("left" -> l, "right" -> r), e.at, F.unit) })
                }
            }
          }
        }
    }
}

object JoinOp {
  val Kind: String = "stream.join"

  final case class Params(leftKey: StatePath, rightKey: StatePath, maxBuffer: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      l <- c.get[String]("leftKey").left.map(_ => "missing field 'leftKey'").flatMap(StatePath.parse)
      r <- c.get[String]("rightKey").left.map(_ => "missing field 'rightKey'").flatMap(StatePath.parse)
      max <- c.getOrElse[Int]("maxBuffer")(10000).left.map(_.message)
      _ <- Either.cond(max > 0, (), "maxBuffer must be more than 0")
    } yield Params(l, r, max)
  }

  /** What each side has seen, by key, and the order of the keys to forget the oldest. */
  final case class Buffers(left: Map[String, Vector[Json]] = Map.empty, right: Map[String, Vector[Json]] = Map.empty, leftOrder: Vector[String] = Vector.empty, rightOrder: Vector[String] = Vector.empty) {

    /** Adds an element of a side; the new state, and the pairs it makes with what the other side had. */
    def add(isLeft: Boolean, key: String, value: Json, max: Int): (Buffers, List[(Json, Json)]) = {
      val (mine, order, other) = if (isLeft) (left, leftOrder, right) else (right, rightOrder, left)
      val present = mine.contains(key)
      val grown = mine.updated(key, mine.getOrElse(key, Vector.empty) :+ value)
      val ordered = if (present) order else order :+ key
      val (kept, keptOrder) =
        if (ordered.size > max) (grown -- ordered.take(ordered.size - max), ordered.drop(ordered.size - max)) else (grown, ordered)
      val pairs = other.getOrElse(key, Vector.empty).toList.map(o => if (isLeft) (value, o) else (o, value))
      val next = if (isLeft) copy(left = kept, leftOrder = keptOrder) else copy(right = kept, rightOrder = keptOrder)
      (next, pairs)
    }
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Join by key",
    description = "Joins two streams by key: gives {left, right} for each pair of elements with the same key.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("leftKey", DataType.Str, description = Some("Path of the key in the elements of `left`")),
        ConfigSchema.Field("rightKey", DataType.Str, description = Some("Path of the key in the elements of `right`")),
        ConfigSchema.Field("maxBuffer", DataType.Num, required = false, default = Some(Json.fromInt(10000)), description = Some("How many keys to keep of each side"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.sides,
    checkParams = p => params(p).left.toOption.toList
  )
}
