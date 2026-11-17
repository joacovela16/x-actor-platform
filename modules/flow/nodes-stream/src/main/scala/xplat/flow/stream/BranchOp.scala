package xplat.flow.stream

import cats.effect.Temporal
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** The `If` of a stream: each element leaves through `then` when a condition (see [[Expr]]) is true for it and through `else` when it
  * is false. An element for which the condition cannot be calculated, or is not true or false, is a dead letter. An element that goes
  * to a port with nothing connected is done there, as if it had been dropped.
  */
final class BranchOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = BranchOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    BranchOp.condition(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(where) =>
        inputs.getOrElse(Port.In, Stream.empty).evalMapFilter { e =>
          where.eval(e.value).flatMap(v => v.asBoolean.toRight(s"the condition is not true or false: ${v.noSpaces}")) match {
            case Right(yes)  => F.pure(Option(e.copy(port = if (yes) BranchOp.Then else BranchOp.Else)))
            case Left(error) => ctx.fail(e, error).as(Option.empty[Elem[F]])
          }
        }
    }
}

object BranchOp {
  val Kind: String = "stream.branch"
  val Then: String = "then"
  val Else: String = "else"

  def condition(params: Json): Either[String, Expr] = FilterOp.condition(params)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Branch (if)",
    description = "Sends each element through 'then' when a condition is true for it and through 'else' when it is false.",
    config = ConfigSchema(List(ConfigSchema.Field("where", DataType.AnyJson, description = Some("Condition, such as {\"gt\": [{\"path\": \"total\"}, 100]}")))),
    ports = _ => List(Port(Then, stream = Some(DataType.AnyJson)), Port(Else, stream = Some(DataType.AnyJson))),
    inputs = _ => StreamPorts.input,
    checkParams = p => condition(p).left.toOption.toList
  )
}
