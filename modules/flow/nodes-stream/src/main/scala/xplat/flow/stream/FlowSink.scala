package xplat.flow.stream

import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.FlowRunner

/** Runs another flow for each element (or for each window, if the stream is of windows). The inputs of the flow are filled from the
  * element: for each one, the path of the field of the element that fills it, or `$` for the whole element. With `wait` (the default)
  * the element is done when that flow ends: if it fails, the element is a dead letter. `parallel` flows run at the same time.
  *
  * It writes to the first output it declares the number of flows that ended well.
  */
final class FlowSink[F[_]](runner: FlowRunner[F])(implicit F: Async[F]) extends StreamSink[F] {

  val descriptor: NodeDescriptor = FlowSink.descriptor

  def drain(ctx: StreamContext[F], upstream: Stream[F, Elem[F]]): F[Map[String, Json]] =
    FlowSink.params(ctx.params) match {
      case Left(problem) => F.raiseError(new IllegalArgumentException(problem))
      case Right(p) =>
        Ref.of[F, Long](0L).flatMap { done =>
          upstream
            .parEvalMapUnordered(p.parallel) { e =>
              FlowSink.inputs(p, e.value) match {
                case Left(error) => ctx.fail(e, error)
                case Right(inputs) =>
                  runner
                    .run(p.flowId, p.revision, inputs, p.waits, FlowRunner.Caller(ctx.input.instanceId, ctx.node.id))
                    .handleError(t => FlowRunner.Failed(Option(t.getMessage).getOrElse(t.toString)))
                    .flatMap {
                      case _: FlowRunner.Finished => done.update(_ + 1) >> e.ack
                      case FlowRunner.Failed(reason, _) => ctx.fail(e, s"flow '${p.flowId}' failed: $reason")
                    }
              }
            }
            .compile
            .drain >> done.get.map(n => ctx.result(Json.fromLong(n)))
        }
    }
}

object FlowSink {
  val Kind: String = "stream.sink.flow"

  final case class Params(flowId: String, revision: Option[Int], inputs: Map[String, String], waits: Boolean, parallel: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      flowId <- c.get[String]("flowId").left.map(_ => "missing field 'flowId'")
      revision <- c.get[Option[Int]]("revision").left.map(_.message)
      inputs <- c.getOrElse[Map[String, String]]("inputs")(Map.empty).left.map(_.message)
      _ <- inputs.values.toList.filter(_ != "$").traverse(StatePath.parse)
      waits <- c.getOrElse[Boolean]("wait")(true).left.map(_.message)
      parallel <- c.getOrElse[Int]("parallel")(1).left.map(_.message)
      _ <- Either.cond(parallel > 0, (), "parallel must be more than 0")
    } yield Params(flowId, revision, inputs, waits, parallel)
  }

  def inputs(p: Params, element: Json): Either[String, Map[String, Json]] =
    p.inputs.toList.traverse { case (name, ref) =>
      if (ref == "$") Right(name -> element)
      else StatePath.parse(ref).flatMap(_.get(element).map(name -> _).toRight(s"'$ref', for the input '$name', is not in the element"))
    }.map(_.toMap)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SinkCategory,
    label = "Run flow for each",
    description = "Ends a stream by running another flow for each element, filling its inputs from the element.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("flowId", DataType.Str, description = Some("The flow to run")),
        ConfigSchema.Field("revision", DataType.Num, required = false, description = Some("Revision of the flow; the latest if empty")),
        ConfigSchema.Field("inputs", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Input of the flow -> path of the field of the element, or $ for all of it")),
        ConfigSchema.Field("wait", DataType.Bool, required = false, default = Some(Json.True), description = Some("The element is done when the flow ends")),
        ConfigSchema.Field("parallel", DataType.Num, required = false, default = Some(Json.fromInt(1)), description = Some("Flows that run at the same time"))
      )
    ),
    sideEffects = SideEffects.Write,
    inputs = _ => StreamPorts.input,
    defaultOutputs = _ => List(OutputDecl("result", StatePath(List("flowsRun")), DataType.Num)),
    checkParams = p => params(p).left.toOption.toList
  )
}
