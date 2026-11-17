package xplat.flow.script

import cats.effect.{Async, Resource, Temporal}
import cats.syntax.all._
import fs2.Stream
import io.circe.{Decoder, Json}
import xplat.flow.model._
import xplat.flow.stream.{Elem, StreamContext, StreamOperator, StreamPorts}

import scala.concurrent.duration._

/** An operator of a stream whose body is JavaScript, for each element. The code is the body of a function of `(element, params,
  * log)`, in the sandbox of [[ScriptSandbox]]. What it returns depends on `mode`:
  *
  *  - `map`: the new element (returning nothing makes the element a dead letter);
  *  - `filter`: whether to keep the element (anything that is true in JavaScript);
  *  - `flatMap`: a list, with one new element for each item (an empty list drops the element).
  *
  * The code is compiled once for the whole segment, and the context is built again after a call that was stopped for taking too
  * long. An element where the script throws, takes too long or returns the wrong thing is a dead letter.
  */
final class StreamScript[F[_]](limits: ScriptSandbox.Limits = ScriptSandbox.Limits())(implicit F: Async[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = StreamScript.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    StreamScript.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        val timeout = p.timeoutMillis.fold(limits.timeout)(ms => ms.millis min limits.timeout)
        val session: Resource[F, ScriptSandbox.Session] = Resource.make(
          F.blocking(ScriptSandbox.open(p.code, StreamScript.Args, limits)).flatMap(_.leftMap(e => new IllegalArgumentException(e.render)).liftTo[F])
        )(s => F.blocking(s.close()))
        Stream.resource(session).flatMap { s =>
          inputs.getOrElse(Port.In, Stream.empty).flatMap { e =>
            Stream.eval(F.blocking(s.call(List(e.value, p.args), timeout))).flatMap {
              case Left(error) => Stream.exec(ctx.fail(e, error.render))
              case Right(result) => StreamScript.emit(ctx, p.mode, e, result.value)
            }
          }
        }
    }
}

object StreamScript {

  val Kind: String = "stream.script"
  val Args: List[String] = List("element", "params")

  val Modes: List[String] = List("map", "filter", "flatMap")

  final case class Params(code: String, mode: String, args: Json, timeoutMillis: Option[Long])

  def params(json: Json): Either[String, Params] = {
    val d: Decoder[Params] = Decoder.instance { c =>
      for {
        code <- c.get[String]("code")
        mode <- c.getOrElse[String]("mode")("map")
        args <- c.getOrElse[Json]("args")(Json.obj())
        timeout <- c.get[Option[Long]]("timeoutMs")
      } yield Params(code, mode, args, timeout)
    }
    json.as(d).left.map(e => s"invalid configuration: ${e.message}").flatMap(p => Either.cond(Modes.contains(p.mode), p, s"mode must be one of ${Modes.mkString(", ")}"))
  }

  private def truthy(v: Json): Boolean =
    v.fold(false, identity, n => n.toDouble != 0.0 && !n.toDouble.isNaN, _.nonEmpty, _ => true, _ => true)

  private def emit[F[_]](ctx: StreamContext[F], mode: String, e: Elem[F], value: Option[Json])(implicit F: Temporal[F]): Stream[F, Elem[F]] =
    mode match {
      case "filter" => if (value.exists(truthy)) Stream.emit(e) else Stream.exec(ctx.drop(e))
      case "map" =>
        value match {
          case Some(v) => Stream.emit(ctx.element(e, v))
          case None    => Stream.exec(ctx.fail(e, "the script returned nothing"))
        }
      case _ =>
        value.flatMap(_.asArray) match {
          case None                         => Stream.exec(ctx.fail(e, "flatMap needs the script to return a list"))
          case Some(items) if items.isEmpty => Stream.exec(ctx.drop(e))
          case Some(items) =>
            val last = items.size - 1
            // The element is done when its last item is: only that one carries its acknowledgement.
            Stream.emits(items.zipWithIndex.map { case (v, i) => e.copy(value = v, ack = if (i == last) e.ack else F.unit) })
        }
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Script (JavaScript)",
    description = "Runs JavaScript for each element of a stream, to map it, filter it or turn it into several.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("code", DataType.Str, description = Some("JavaScript: the body of a function of (element, params, log)."), ui = Some(UiSpec.code("javascript"))),
        ConfigSchema.Field("mode", DataType.Str, required = false, default = Some(Json.fromString("map")), oneOf = Modes.map(Json.fromString), description = Some("map returns the new element, filter returns whether to keep it, flatMap returns a list")),
        ConfigSchema.Field("args", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("A value the script sees as `params`")),
        ConfigSchema.Field("timeoutMs", DataType.Num, required = false, description = Some("Time limit for each element, in milliseconds"))
      )
    ),
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = json =>
      params(json) match {
        case Left(problem) => List(problem)
        case Right(p)      => ScriptSandbox.check(p.code, Args).map(e => s"code: ${e.render}").toList
      }
  )
}
