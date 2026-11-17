package xplat.flow.stream

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.FlowRunner

/** Runs another flow for each element and passes on what it answered, like `evalMap` (and `parEvalMap`) of fs2: the effect of each
  * element is a flow, which can query a database, call a service or do anything a flow does. It is how a stream is enriched: read
  * an order, look its customer up, publish both.
  *
  * The inputs of the flow are filled from the element, as in [[FlowSink]] (a path of the element, or `$` for all of it). The outputs
  * it exposes, as an object by name, either replace the element or, with `into`, are set at that path of it. Up to `parallel` flows
  * run at once; with `ordered` (the default) the elements leave in the order they came, otherwise as their flows end. A flow that
  * fails makes its element a dead letter.
  *
  * In a simulation no flow is run: the element passes with an empty object for the outputs.
  */
final class EvalMapOp[F[_]](runner: FlowRunner[F])(implicit F: Async[F]) extends StreamOperator[F] {

  val descriptor: NodeDescriptor = EvalMapOp.descriptor

  def apply(ctx: StreamContext[F], inputs: Map[String, Stream[F, Elem[F]]]): Stream[F, Elem[F]] =
    EvalMapOp.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        val in = inputs.getOrElse(Port.In, Stream.empty)
        val each: Elem[F] => F[Option[Elem[F]]] = e => call(ctx, p, e)
        val out = if (p.ordered) in.parEvalMap(p.parallel)(each) else in.parEvalMapUnordered(p.parallel)(each)
        out.unNone
    }

  private def call(ctx: StreamContext[F], p: EvalMapOp.Params, e: Elem[F]): F[Option[Elem[F]]] =
    FlowSink.inputs(p.flow, e.value) match {
      case Left(error) => ctx.fail(e, error).as(None)
      case Right(args) =>
        val result: F[FlowRunner.Result] =
          if (ctx.simulated) F.pure(FlowRunner.Finished(Map.empty))
          else
            runner
              .run(p.flow.flowId, p.flow.revision, args, wait = true, FlowRunner.Caller(ctx.input.instanceId, ctx.node.id))
              .handleError(t => FlowRunner.Failed(Option(t.getMessage).getOrElse(t.toString)))
        result.flatMap {
          case FlowRunner.Finished(outputs, _) =>
            EvalMapOp.merge(p.into, e.value, Json.fromFields(outputs)) match {
              case Right(v)    => F.pure(Some(ctx.element(e, v)))
              case Left(error) => ctx.fail(e, error).as(None)
            }
          case FlowRunner.Failed(reason, _) => ctx.fail(e, s"flow '${p.flow.flowId}' failed: $reason").as(None)
        }
    }
}

object EvalMapOp {
  val Kind: String = "stream.evalMap"

  final case class Params(flow: FlowSink.Params, into: Option[StatePath], ordered: Boolean) {
    def parallel: Int = flow.parallel
  }

  def params(json: Json): Either[String, Params] =
    for {
      flow <- FlowSink.params(json)
      into <- StreamPorts.text(json, "into").traverse(StatePath.parse)
      ordered <- json.hcursor.getOrElse[Boolean]("ordered")(true).left.map(_.message)
    } yield Params(flow, into, ordered)

  /** The element after the flow: its outputs in its place, or set at `into` of it. */
  def merge(into: Option[StatePath], element: Json, outputs: Json): Either[String, Json] =
    into match {
      case None => Right(outputs)
      case Some(path) =>
        if (element.isObject) Right(path.set(element, outputs))
        else Left(s"the element is not an object, so the outputs cannot go into '${path.render}'")
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.OperatorCategory,
    label = "Run flow per element (evalMap)",
    description =
      "Runs another flow for each element and passes on its outputs, in place of the element or inside it (fs2 evalMap / parEvalMap). Use it to look something up for each element.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("flowId", DataType.Str, description = Some("The flow to run")),
        ConfigSchema.Field("revision", DataType.Num, required = false, description = Some("Revision of the flow; the latest if empty")),
        ConfigSchema.Field("inputs", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Input of the flow -> path of the field of the element, or $ for all of it")),
        ConfigSchema.Field("into", DataType.Str, required = false, description = Some("Path in the element for the outputs of the flow; empty to replace the element")),
        ConfigSchema.Field("parallel", DataType.Num, required = false, default = Some(Json.fromInt(1)), description = Some("Flows that run at the same time")),
        ConfigSchema.Field("ordered", DataType.Bool, required = false, default = Some(Json.True), description = Some("Elements leave in the order they came; otherwise as their flows end"))
      )
    ),
    sideEffects = SideEffects.Write,
    ports = _ => StreamPorts.output(),
    inputs = _ => StreamPorts.input,
    checkParams = p => params(p).left.toOption.toList
  )
}
