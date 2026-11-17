package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** Passes elements on while a condition (see [[Expr]]) is true for them, like `takeWhile` of fs2: the first one for which it is
  * false is dropped and the stream ends, stopping the nodes before it. An element for which it cannot be calculated is a dead letter. */
final class TakeWhileOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = TakeWhileOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    StreamPorts.condition(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(where) =>
        in.evalMap(e => StreamPorts.holds(where, e).flatMap {
            case Right(true)  => F.pure(Option(Option(e)))
            case Right(false) => ctx.drop(e).as(Option.empty[Option[Elem[F]]])
            case Left(error)  => ctx.fail(e, error).as(Option(Option.empty[Elem[F]]))
          })
          .unNoneTerminate
          .unNone
    }
  }
}

object TakeWhileOp {
  val Kind: String = "stream.takeWhile"

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Take while",
    description = "Passes elements on while a condition holds; at the first one for which it does not, the stream ends (fs2 takeWhile).",
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
