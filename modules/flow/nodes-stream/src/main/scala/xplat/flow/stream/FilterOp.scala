package xplat.flow.stream

import cats.effect.Temporal
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Keeps the elements for which a condition (see [[Expr]]) is true and drops the others. An element for which it cannot be calculated,
  * or is not true or false, is a dead letter.
  */
final class FilterOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = FilterOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    FilterOp.condition(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(where) =>
        inputs.getOrElse(Port.In, Stream.empty).evalMapFilter { e =>
          where.eval(e.value).flatMap(v => v.asBoolean.toRight(s"the condition is not true or false: ${v.noSpaces}")) match {
            case Right(true)  => F.pure(Option(e))
            case Right(false) => ctx.drop(e).as(Option.empty[Elem[F]])
            case Left(error)  => ctx.fail(e, error).as(Option.empty[Elem[F]])
          }
        }
    }
}

object FilterOp {
  val Kind: String = "stream.filter"

  def condition(params: Json): Either[String, Expr] =
    params.hcursor.get[Json]("where").left.map(_ => "missing field 'where'").flatMap(Expr.parse(_).left.map(m => s"where: $m"))

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Filter",
    description = "Keeps the elements for which a condition is true and drops the others.",
    config = ConfigSchema(List(ConfigSchema.Field("where", DataType.AnyJson, description = Some("Condition, such as {\"gt\": [{\"path\": \"total\"}, 100]}")))),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => condition(p).left.toOption.toList
  )
}
