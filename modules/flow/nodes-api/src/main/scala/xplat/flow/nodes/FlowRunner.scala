package xplat.flow.nodes

import io.circe.Json
import xplat.flow.model.StatePath

/** What the engine offers to the nodes that run other flows. */
trait FlowRunner[F[_]] {

  /** Starts the flow with the inputs filled, each under its name.
    *
    * With `wait` it answers when the flow ends, with the outputs it exposes by name, or the reason it failed. Without it,
    * it answers as soon as the flow is started, with no outputs.
    */
  def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: FlowRunner.Caller): F[FlowRunner.Result]
}

object FlowRunner {

  /** Who asks: the node of an instance, which the started instance records as its origin. */
  final case class Caller(instanceId: String, nodeId: String)

  /** How the called flow went. `childInstanceId` is the instance that ran it, for the audit trail. */
  sealed trait Result
  final case class Finished(outputs: Map[String, Json], childInstanceId: Option[String] = None) extends Result
  final case class Failed(reason: String, childInstanceId: Option[String] = None) extends Result

  /** A runner that starts nothing: every call fails. */
  def none[F[_]](implicit F: cats.Applicative[F]): FlowRunner[F] = new FlowRunner[F] {
    def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: Caller): F[Result] =
      F.pure(Failed("there is no engine to run flows"))
  }

  /** A runner that waits for the real one: for the nodes built before the engine that runs them. */
  def later[F[_]](runner: F[FlowRunner[F]])(implicit F: cats.FlatMap[F]): FlowRunner[F] = new FlowRunner[F] {
    def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: Caller): F[Result] =
      F.flatMap(runner)(_.run(flowId, revision, inputs, wait, caller))
  }

  /** The value at each path of `state`, under the name of the input it fills. A path that is not there is a problem. */
  private[nodes] def collect(state: Json, bindings: Map[String, StatePath]): Either[String, Map[String, Json]] =
    bindings.toList.sortBy(_._1).foldLeft[Either[String, Map[String, Json]]](Right(Map.empty)) { case (acc, (name, path)) =>
      acc.flatMap(m => path.get(state).map(v => m + (name -> v)).toRight(s"'$path', for the input '$name', is not in the state"))
    }
}
