package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Groups elements by a key and calculates, for each group, counts, sums and the like.
  *
  *   - `over: items` takes each element to be a window (see [[WindowOp]]) and gives one element for each group in its `items`.
  *   - `over: stream` calculates over the whole stream: with `emit: end` one element for each group when the stream ends, with
  *     `emit: each` the new value of the group after every element.
  *
  * The element is `{"key": <the key>, <name>: <value>, ...}` (`key` only if there is a key). The operations are `count`, `sum`, `avg`,
  * `min`, `max`, `first`, `last` and `collect` (a list, of at most `limit` values). What it keeps is in memory, so elements are
  * acknowledged as they come in.
  */
final class AggregateOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = AggregateOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    AggregateOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        val in = inputs.getOrElse(Port.In, Stream.empty)
        p.over match {
          case "items" =>
            in.flatMap { e =>
              e.value.hcursor.get[List[Json]]("items") match {
                case Left(_) => Stream.exec(ctx.fail(e, "the element has no list 'items': is it a window?"))
                case Right(items) =>
                  AggregateOp.groups(p, items) match {
                    case Left(error) => Stream.exec(ctx.fail(e, error))
                    case Right(rows) =>
                      val last = rows.size - 1
                      if (rows.isEmpty) Stream.exec(ctx.drop(e))
                      else Stream.emits(rows.zipWithIndex.map { case (r, i) => e.copy(value = r, ack = if (i == last) e.ack else F.unit) })
                  }
              }
            }
          case "stream" if p.emit == "each" =>
            Stream.eval(Ref.of[F, AggregateOp.State](AggregateOp.State())).flatMap { ref =>
              in.evalMapFilter { e =>
                ref.modify { s =>
                  AggregateOp.add(p, s, e.value) match {
                    case Left(error)         => (s, Left(error))
                    case Right((next, key)) => (next, Right(AggregateOp.row(p, next, key)))
                  }
                }.flatMap {
                  case Left(error) => ctx.fail(e, error).as(Option.empty[Elem[F]])
                  case Right(row)  => F.pure(Option(e.withValue(row)))
                }
              }
            }
          case _ =>
            Stream.eval(Ref.of[F, AggregateOp.State](AggregateOp.State())).flatMap { ref =>
              val folding = in.evalMap { e =>
                ref.modify { s =>
                  AggregateOp.add(p, s, e.value) match {
                    case Left(error)         => (s, Some(error))
                    case Right((next, _)) => (next, None)
                  }
                }.flatMap(_.fold(e.ack)(ctx.fail(e, _)))
              }.drain
              folding ++ Stream.eval(ref.get.product(ctx.now)).flatMap { case (s, at) =>
                Stream.emits(s.order.map(key => Elem.of[F](AggregateOp.row(p, s, key), at)))
              }
            }
        }
    }
}

object AggregateOp {
  val Kind: String = "stream.aggregate"

  final case class Spec(name: String, op: String, path: Option[StatePath])

  final case class Params(key: Option[StatePath], over: String, emit: String, specs: List[Spec], limit: Int)

  private val Ops = Set("count", "sum", "avg", "min", "max", "first", "last", "collect")

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      key <- c.get[Option[String]]("key").left.map(_.message).flatMap(_.filter(_.nonEmpty).traverse(StatePath.parse))
      over <- c.getOrElse[String]("over")("stream").left.map(_.message)
      emit <- c.getOrElse[String]("emit")("end").left.map(_.message)
      limit <- c.getOrElse[Int]("limit")(1000).left.map(_.message)
      aggs <- c.getOrElse[Map[String, Json]]("aggregations")(Map.empty).left.map(_.message)
      specs <- aggs.toList.sortBy(_._1).traverse { case (name, spec) =>
        for {
          op <- spec.hcursor.get[String]("op").left.map(_ => s"aggregations.$name: missing 'op'")
          _ <- Either.cond(Ops(op), (), s"aggregations.$name: unknown op '$op'")
          path <- spec.hcursor.get[Option[String]]("path").left.map(_.message).flatMap(_.filter(_.nonEmpty).traverse(StatePath.parse))
          _ <- Either.cond(op == "count" || path.isDefined, (), s"aggregations.$name: '$op' needs a 'path'")
        } yield Spec(name, op, path)
      }
      _ <- Either.cond(specs.nonEmpty, (), "aggregations: at least one is needed")
      _ <- Either.cond(Set("items", "stream")(over), (), s"over must be 'items' or 'stream', not '$over'")
      _ <- Either.cond(Set("end", "each")(emit), (), s"emit must be 'end' or 'each', not '$emit'")
    } yield Params(key, over, emit, specs, limit)
  }

  /** What is known of one aggregation of one group. */
  final case class Acc(count: Long = 0, sum: BigDecimal = 0, min: Option[Json] = None, max: Option[Json] = None, first: Option[Json] = None, last: Option[Json] = None, items: Vector[Json] = Vector.empty)

  final case class State(groups: Map[Json, Map[String, Acc]] = Map.empty, order: Vector[Json] = Vector.empty)

  private def step(spec: Spec, limit: Int, acc: Acc, element: Json): Either[String, Acc] =
    spec.path match {
      case None => Right(acc.copy(count = acc.count + 1))
      case Some(path) =>
        path.get(element).toRight(s"'$path' is not in the element").flatMap { v =>
          val counted = acc.copy(count = acc.count + 1, first = acc.first.orElse(Some(v)), last = Some(v))
          spec.op match {
            case "sum" | "avg" => v.asNumber.flatMap(_.toBigDecimal).toRight(s"'$path' is not a number: ${v.noSpaces}").map(n => counted.copy(sum = acc.sum + n))
            case "min"         => acc.min.fold[Either[String, Option[Json]]](Right(Some(v)))(m => Expr.compare(v, m).map(c => Some(if (c < 0) v else m))).map(m => counted.copy(min = m))
            case "max"         => acc.max.fold[Either[String, Option[Json]]](Right(Some(v)))(m => Expr.compare(v, m).map(c => Some(if (c > 0) v else m))).map(m => counted.copy(max = m))
            case "collect"     => Right(if (acc.items.size < limit) counted.copy(items = acc.items :+ v) else counted)
            case _             => Right(counted)
          }
        }
    }

  private def result(spec: Spec, acc: Acc): Json = spec.op match {
    case "count"   => Json.fromLong(acc.count)
    case "sum"     => Json.fromBigDecimal(acc.sum)
    case "avg"     => if (acc.count == 0) Json.Null else Json.fromBigDecimal(acc.sum / acc.count)
    case "min"     => acc.min.getOrElse(Json.Null)
    case "max"     => acc.max.getOrElse(Json.Null)
    case "first"   => acc.first.getOrElse(Json.Null)
    case "last"    => acc.last.getOrElse(Json.Null)
    case "collect" => Json.fromValues(acc.items)
    case _         => Json.Null
  }

  /** The state after an element, and the key of its group. Nothing changes if the element cannot be taken in. */
  def add(p: Params, s: State, element: Json): Either[String, (State, Json)] =
    for {
      key <- p.key.fold[Either[String, Json]](Right(Json.Null))(k => k.get(element).toRight(s"'$k' is not in the element"))
      current = s.groups.getOrElse(key, Map.empty)
      updated <- p.specs.foldLeft[Either[String, Map[String, Acc]]](Right(current)) { (acc, spec) =>
        acc.flatMap(m => step(spec, p.limit, m.getOrElse(spec.name, Acc()), element).map(a => m.updated(spec.name, a)))
      }
    } yield (State(s.groups.updated(key, updated), if (s.groups.contains(key)) s.order else s.order :+ key), key)

  def row(p: Params, s: State, key: Json): Json = {
    val accs = s.groups.getOrElse(key, Map.empty)
    val values = p.specs.map(spec => spec.name -> result(spec, accs.getOrElse(spec.name, Acc())))
    Json.fromFields((if (p.key.isDefined) List("key" -> key) else Nil) ++ values)
  }

  /** The rows for the items of a window. */
  def groups(p: Params, items: List[Json]): Either[String, List[Json]] =
    items.foldLeft[Either[String, State]](Right(State()))((acc, item) => acc.flatMap(add(p, _, item).map(_._1))).map(s => s.order.toList.map(row(p, s, _)))

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Aggregate",
    description = "Groups the elements by a key and calculates counts, sums, averages and the like for each group.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("aggregations", DataType.AnyJson, description = Some("Name -> {op: count|sum|avg|min|max|first|last|collect, path}")),
        ConfigSchema.Field("key", DataType.Str, required = false, description = Some("Path of the key to group by")),
        ConfigSchema.Field("over", DataType.Str, required = false, default = Some(Json.fromString("stream")), oneOf = List(Json.fromString("stream"), Json.fromString("items"))),
        ConfigSchema.Field("emit", DataType.Str, required = false, default = Some(Json.fromString("end")), oneOf = List(Json.fromString("end"), Json.fromString("each"))),
        ConfigSchema.Field("limit", DataType.Num, required = false, default = Some(Json.fromInt(1000)), description = Some("Most values a `collect` keeps"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )
}
