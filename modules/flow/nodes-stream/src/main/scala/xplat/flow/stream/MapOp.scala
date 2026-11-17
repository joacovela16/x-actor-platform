package xplat.flow.stream

import cats.effect.Temporal
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Turns each element into another: it sets fields calculated from the element, removes fields, or replaces the element by the
  * value of an expression (see [[Expr]]). An element it cannot calculate (a field that is not there) is a dead letter.
  */
final class MapOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = MapOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    MapOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        inputs.getOrElse(Port.In, Stream.empty).evalMapFilter { e =>
          MapOp.transform(p, e.value) match {
            case Right(v)    => F.pure(Option(ctx.element(e, v)))
            case Left(error) => ctx.fail(e, error).as(Option.empty[Elem[F]])
          }
        }
    }
}

object MapOp {
  val Kind: String = "stream.map"

  final case class Params(set: List[(StatePath, Expr)], remove: List[StatePath], replace: Option[Expr])

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      set <- c.getOrElse[Map[String, Json]]("set")(Map.empty).left.map(_.message).flatMap { fields =>
        fields.toList.sortBy(_._1).traverse { case (name, e) => (StatePath.parse(name), Expr.parse(e).left.map(m => s"set.$name: $m")).tupled }
      }
      remove <- c.getOrElse[List[StatePath]]("remove")(Nil).left.map(_.message)
      replace <- c.get[Option[Json]]("replace").left.map(_.message).flatMap(_.traverse(Expr.parse))
    } yield Params(set, remove, replace)
  }

  def transform(p: Params, element: Json): Either[String, Json] =
    for {
      base <- p.replace.fold[Either[String, Json]](Right(element))(_.eval(element))
      withSet <- p.set.foldLeft[Either[String, Json]](Right(base)) { case (acc, (path, expr)) =>
        for {
          current <- acc
          _ <- Either.cond(current.isObject, (), "only an object can have fields set")
          value <- expr.eval(element)
        } yield path.set(current, value)
      }
    } yield p.remove.foldLeft(withSet)((j, path) => removeAt(j, path.segments))

  private def removeAt(json: Json, segments: List[String]): Json = segments match {
    case Nil          => json
    case last :: Nil  => json.mapObject(_.remove(last))
    case head :: rest => json.mapObject(o => o(head).fold(o)(inner => o.add(head, removeAt(inner, rest))))
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Map",
    description = "Turns each element into another: sets calculated fields, removes fields, or replaces the element.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("set", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Field -> expression, such as {\"total\": {\"mul\": [{\"path\": \"price\"}, {\"path\": \"qty\"}]}}")),
        ConfigSchema.Field("remove", DataType.ListOf(DataType.Str), required = false, default = Some(Json.arr())),
        ConfigSchema.Field("replace", DataType.AnyJson, required = false, description = Some("Expression whose value replaces the element"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )
}
