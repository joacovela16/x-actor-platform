package xplat.flow.nodes

import xplat.flow.model.{NodeDescriptor, NodeInput, NodeOutput}

/** A kind of node that can run: its description and the code that executes it.
  *
  * `run` is a plain function of its input, so a node can be tested without actors. A node that fails because of its
  * input answers [[NodeOutput.failed]]; an exception in `F` is for the unexpected.
  */
trait NodeType[F[_]] {
  def descriptor: NodeDescriptor

  def run(input: NodeInput): F[NodeOutput]

  /** What the node would answer in a simulation, without touching anything outside, when it can say it. A node that writes to
    * an external system returns what it would have sent in `info` (the audit shows it); `None` leaves the engine to answer
    * with default values for the outputs the node declares.
    */
  def simulated(input: NodeInput): Option[NodeOutput] = None

  /** Whether the node can run for as long as it takes, such as a stream that does not end: the engine does not give it a
    * time limit. It is stopped when the instance is cancelled.
    */
  def unbounded: Boolean = false
}
