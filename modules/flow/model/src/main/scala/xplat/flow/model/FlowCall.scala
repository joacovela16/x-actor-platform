package xplat.flow.model

/** That a node runs another flow, as the node's configuration says it.
  *
  * @param revision
  *   the revision of the called flow, or the latest one when it is empty.
  * @param inputs
  *   the inputs of the called flow, by name, with the path in the state of the caller that each one is filled from.
  * @param waits
  *   whether the caller waits for the called flow to finish and receives its outputs. If not, the called flow runs on its
  *   own and the caller goes on at once.
  */
final case class FlowCall(flowId: String, revision: Option[Int], inputs: Map[String, StatePath], waits: Boolean)
