package xplat.flow.engine

import io.circe.Json
import xplat.flow.model.{FlowDef, NodeCatalog}

/** Runs flows. An instance is one run of a flow: it has its own state, and it survives a restart because it is rebuilt from
  * its events.
  */
trait FlowEngine[F[_]] {

  /** Starts an instance of the flow, the latest revision if none is given. `inputs` is an object with one value for each
    * input the flow declares, by name. With an `instanceId` that already exists nothing is started again and the same id is
    * returned, so a request that is repeated does not run twice.
    *
    * It answers once the instance has started, not when it ends. It fails with [[FlowEngine.FlowNotFound]] or
    * [[FlowEngine.InvalidInputs]].
    */
  def start(flowId: String, revision: Option[Int], inputs: Json, instanceId: Option[String], origin: Origin = Origin.Manual()): F[InstanceId]

  /** The instance as it is now, or nothing if there is no such instance. */
  def status(id: InstanceId): F[Option[InstanceStatus]]

  /** Stops an instance that is running: what runs is cancelled (a stream that does not end stops), what was waiting does not
    * run, and the instance ends as failed. Nothing happens to one that has ended. It fails with
    * [[FlowEngine.InstanceNotFound]].
    */
  def cancel(id: InstanceId): F[Unit]

  /** Runs the instance again, which has to have ended. Each node follows its run policy: one that succeeded last time and is
    * set to skip is not run again, unless its configuration or what it reads changed.
    *
    * With `useLatest` it takes the latest revision of the flow, which is how an edited flow is picked up; otherwise it uses
    * the definition the instance has.
    */
  def retry(id: InstanceId, useLatest: Boolean = false, by: Option[String] = None): F[Unit]

  /** The audit entries of the instance from the start, and the new ones as they come, until it ends. They are the same ones
    * that [[audit]] returns, so a screen that follows an instance live shows what one that reads it afterwards does.
    */
  def events(id: InstanceId): fs2.Stream[F, AuditEntry]

  /** The audit trail of the instance: what it is and everything that happened, in order. Nothing if there is no such
    * instance.
    */
  def audit(id: InstanceId): F[Option[AuditTrail]]

  /** Searches the instances that the engine knows, newest first. */
  def search(query: InstanceQuery): F[Page[InstanceSummary]]

  /** Simulates the flow, which need not be saved, without effects outside. `mocks` are by node id. See [[Simulator]]. */
  def simulate(flow: FlowDef, inputs: Json, mocks: Map[String, Mock], realReads: Boolean = false): F[Trace]

  /** The kinds of node that this engine can run. */
  def catalog: NodeCatalog

  /** The store of the definitions this engine runs. */
  def store: FlowStore[F]

  /** The entities of the business model that the types of flows can name, as they are now. */
  def types: F[xplat.flow.model.Types]

  /** Starts other flows for the nodes that call them, recording the calling node as the origin of the new instance. */
  def runner: xplat.flow.nodes.FlowRunner[F]
}

object FlowEngine {

  sealed abstract class Error(message: String) extends RuntimeException(message)
  final case class FlowNotFound(flowId: String, revision: Option[Int]) extends Error(s"flow '$flowId'${revision.fold("")(r => s" revision $r")} does not exist")
  final case class InvalidInputs(problem: String) extends Error(s"invalid inputs: $problem")
  final case class InstanceNotFound(id: InstanceId) extends Error(s"instance '$id' does not exist")
  final case class NotFinished(id: InstanceId) extends Error(s"instance '$id' is still running")
}
