package xplat.flow.engine

import xplat.flow.model.{FlowDef, FlowError, FlowResolver}

/** Where the definitions of the flows are kept, with every revision that was saved. This is the interface; an
  * implementation that keeps them in a database can replace [[InMemoryFlowStore]].
  */
trait FlowStore[F[_]] {

  /** Validates the flow and saves it as a new revision, which is one more than the latest. The `revision` it comes with is
    * ignored. Nothing is saved if it is not valid.
    */
  def save(flow: FlowDef): F[Either[List[FlowError], FlowDef]]

  /** The flow with that id and revision, or its latest revision if none is given. */
  def get(id: String, revision: Option[Int]): F[Option[FlowDef]]

  /** The latest revision of each flow. */
  def list: F[List[FlowSummary]]

  /** Every revision of the flow, oldest first. */
  def revisions(id: String): F[List[FlowDef]]

  /** Deletes the flow with all its revisions. Flows that call it are not checked again. */
  def delete(id: String): F[Unit]

  /** Finds flows by id and revision, as the validator needs it to check the flows that call others. */
  def resolver: F[FlowResolver]
}
