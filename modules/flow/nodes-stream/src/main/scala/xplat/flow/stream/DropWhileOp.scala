package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Leaves out elements while a condition (see [[Expr]]) is true for them, like `dropWhile` of fs2, and passes on everything from the
  * first one for which it is false, without checking again. An element for which it cannot be calculated is a dead letter. */
final class DropWhileOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = DropWhileOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    StreamPorts.condition(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(where) =>
        Stream.eval(Ref.of[F, Boolean](true)).flatMap { dropping =>
          in.evalMapFilter { e =>
            dropping.get.flatMap {
              case false => F.pure(Option(e))
              case true =>
                StreamPorts.holds(where, e).flatMap {
                  case Right(true)  => ctx.drop(e).as(Option.empty[Elem[F]])
                  case Right(false) => dropping.set(false).as(Option(e))
                  case Left(error)  => ctx.fail(e, error).as(Option.empty[Elem[F]])
                }
            }
          }
        }
    }
  }
}

object DropWhileOp {
  val Kind: String = "stream.dropWhile"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Drop while",
    description = "Leaves out elements while a condition holds, and passes on everything from the first one for which it does not (fs2 dropWhile).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("where", DataType.AnyJson, description = Some("Condition, such as {\"lt\": [{\"path\": \"total\"}, 100]}"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => StreamPorts.condition(p).left.toOption.toList
  )
}
