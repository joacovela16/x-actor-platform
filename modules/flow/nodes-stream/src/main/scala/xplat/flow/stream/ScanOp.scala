package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._

/** A running value, like `scan` of fs2: it starts at `init`, and for each element `step` calculates the new one from `acc` (the value so
  * far) and `elem` (the element), as in `{"add": [{"path": "acc"}, {"path": "elem.total"}]}`. Each new value is passed on in place of
  * the element. Unlike fs2, `init` itself is not passed on: it is not made of any element. An element for which `step` cannot be
  * calculated is a dead letter, and the value stays as it was. */
final class ScanOp[F[_]](implicit F: Temporal[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = ScanOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] = {
    val in = inputs.getOrElse(Port.In, Stream.empty)
    ScanOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right((init, step)) =>
        Stream.eval(Ref.of[F, Json](init)).flatMap { acc =>
          in.evalMapFilter { e =>
            acc.get.flatMap { current =>
              step.eval(Json.obj("acc" -> current, "elem" -> e.value)) match {
                case Right(next) => acc.set(next).as(Option(ctx.element(e, next)))
                case Left(error) => ctx.fail(e, error).as(Option.empty[Elem[F]])
              }
            }
          }
        }
    }
  }
}

object ScanOp {
  val Kind: String = "stream.scan"

  def params(json: Json): Either[String, (Json, Expr)] =
    for {
      init <- json.hcursor.getOrElse[Json]("init")(Json.fromInt(0)).left.map(_.message)
      step <- json.hcursor.get[Json]("step").left.map(_ => "missing field 'step'").flatMap(Expr.parse(_).left.map(m => s"step: $m"))
    } yield (init, step)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Scan",
    description = "Keeps a running value: for each element, step calculates the new one from acc and elem, and passes it on (fs2 scan).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("init", DataType.AnyJson, required = false, default = Some(Json.fromInt(0)), description = Some("The value to start from")),
        ConfigSchema.Field("step", DataType.AnyJson, description = Some("The new value, from acc (the value so far) and elem (the element)"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )
}
