package xplat.flow.nodes

import cats.Applicative
import io.circe.{Decoder, Json}
import xplat.flow.model._

/** Runs another flow as one step. The flow it calls is a black box with typed inputs and outputs: the node fills its
  * inputs from the state and, if it waits, takes its outputs into the state.
  *
  * The outputs that the node declares are named after the outputs the called flow exposes; `target` says where they go
  * in the state of the caller.
  */
final class SubflowNode[F[_]: Applicative](runner: FlowRunner[F]) extends NodeType[F] {
  import cats.syntax.applicative._
  import cats.syntax.functor._

  val descriptor: NodeDescriptor = SubflowNode.descriptor

  def run(input: NodeInput): F[NodeOutput] =
    input.params.as[SubflowNode.Params] match {
      case Left(error) => NodeOutput.failed(s"invalid configuration: ${error.message}").pure[F]
      case Right(params) =>
        FlowRunner.collect(input.state, params.inputs) match {
          case Left(problem) => NodeOutput.failed(problem).pure[F]
          case Right(inputs) =>
            runner.run(params.flowId, params.revision, inputs, params.waits, FlowRunner.Caller(input.instanceId, input.nodeId)).map {
              case FlowRunner.Finished(outputs, child) => SubflowNode.withChild(NodeOutput.ok(outputs), child)
              case FlowRunner.Failed(reason, child)    => SubflowNode.withChild(NodeOutput.failed(s"flow '${params.flowId}' failed: $reason"), child)
            }
        }
    }
}

object SubflowNode {

  /** The key under which the node tells which instance ran the flow, in [[NodeOutput.info]]. */
  val ChildInstance: String = "childInstanceId"

  private[nodes] def withChild(out: NodeOutput, child: Option[String]): NodeOutput =
    child.fold(out)(id => out.copy(info = out.info + (ChildInstance -> Json.fromString(id))))

  val Kind: String = "core.subflow"

  /** @param revision
    *   a revision to use; the latest when it is empty.
    * @param waits
    *   waits for the flow to end and takes its outputs; if not, the flow runs on its own.
    * @param inputs
    *   for each input of the called flow, the path in the state of this flow that fills it.
    */
  final case class Params(flowId: String, revision: Option[Int] = None, waits: Boolean = true, inputs: Map[String, StatePath] = Map.empty) {
    def call: FlowCall = FlowCall(flowId, revision, inputs, waits)
  }

  object Params {
    implicit val decoder: Decoder[Params] = Decoder.instance { c =>
      for {
        flowId <- c.get[String]("flowId")
        revision <- c.get[Option[Int]]("revision")
        waits <- c.getOrElse[Boolean]("wait")(true)
        inputs <- c.getOrElse[Map[String, StatePath]]("inputs")(Map.empty)
      } yield Params(flowId, revision, waits, inputs)
    }
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Flow control",
    label = "Run flow",
    description = "Runs another flow as one step: fills its inputs from the state and, if it waits, puts its outputs into the state.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("flowId", DataType.Str, description = Some("The flow to run")),
        ConfigSchema.Field("revision", DataType.Num, required = false, description = Some("Revision of the flow; the latest if empty")),
        ConfigSchema.Field("wait", DataType.Bool, required = false, default = Some(Json.True), description = Some("Wait for the flow to end and take its outputs")),
        ConfigSchema.Field("inputs", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Input of the flow -> path in this state that fills it"))
      )
    ),
    // Whatever the called flow does is done for real, so in test mode the node is simulated like any other that writes.
    sideEffects = SideEffects.Write,
    checkParams = params => params.as[Params].left.toOption.map(e => s"${e.message}").toList,
    calls = params => params.as[Params].toOption.map(_.call)
  )
}
