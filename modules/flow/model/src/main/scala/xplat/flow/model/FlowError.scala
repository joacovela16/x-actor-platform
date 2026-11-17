package xplat.flow.model

import io.circe.{Encoder, Json}

/** A problem that makes a flow invalid. The validator reports all of them at once. */
sealed trait FlowError {

  /** A short stable name, for the editor to tell the kinds of problem apart. */
  def code: String

  /** The node the problem is about, if it is about one. */
  def node: Option[String]

  def message: String
}

object FlowError {

  final case class UnsupportedVersion(found: Int, supported: Int) extends FlowError {
    val code = "unsupported-version"
    val node: Option[String] = None
    def message = s"version $found of the format is not supported; this server reads version $supported"
  }

  final case class DuplicateNode(id: String) extends FlowError {
    val code = "duplicate-node"
    val node: Option[String] = Some(id)
    def message = s"there are several nodes called '$id'"
  }

  final case class UnknownKind(id: String, kind: String) extends FlowError {
    val code = "unknown-kind"
    val node: Option[String] = Some(id)
    def message = s"node '$id' is of kind '$kind', which does not exist"
  }

  final case class InvalidParams(id: String, problems: List[String]) extends FlowError {
    val code = "invalid-params"
    val node: Option[String] = Some(id)
    def message = s"node '$id' is badly configured: ${problems.mkString("; ")}"
  }

  final case class DuplicateOutput(id: String, name: String) extends FlowError {
    val code = "duplicate-output"
    val node: Option[String] = Some(id)
    def message = s"node '$id' declares the output '$name' more than once"
  }

  final case class UnknownNode(edge: Edge, missing: String) extends FlowError {
    val code = "unknown-node"
    val node: Option[String] = None
    def message = s"the edge ${edge.from} -> ${edge.to} uses the node '$missing', which does not exist"
  }

  final case class UnknownPort(id: String, port: String, available: List[String]) extends FlowError {
    val code = "unknown-port"
    val node: Option[String] = Some(id)
    def message = s"node '$id' has no port '$port'; its ports are ${available.mkString(", ")}"
  }

  /** The nodes that are part of a cycle or that wait for one. */
  final case class Cycle(nodes: List[String]) extends FlowError {
    val code = "cycle"
    val node: Option[String] = None
    def message = s"the flow has a cycle among ${nodes.mkString(", ")}"
  }

  final case class StateNotDeclared(id: String, path: StatePath) extends FlowError {
    val code = "state-not-declared"
    val node: Option[String] = Some(id)
    def message = s"node '$id' reads '$path', which no node before it, nor the inputs of the flow, declares"
  }

  final case class StateTypeMismatch(id: String, path: StatePath, expected: DataType, found: DataType) extends FlowError {
    val code = "state-type-mismatch"
    val node: Option[String] = Some(id)
    def message = s"node '$id' needs '$path' to be ${expected.render}, but it is declared as ${found.render}"
  }

  final case class UnknownFlow(id: String, flowId: String, revision: Option[Int]) extends FlowError {
    val code = "unknown-flow"
    val node: Option[String] = Some(id)
    def message = s"node '$id' calls the flow '$flowId'${revision.fold("")(r => s" revision $r")}, which does not exist"
  }

  final case class UnknownFlowInput(id: String, name: String) extends FlowError {
    val code = "unknown-flow-input"
    val node: Option[String] = Some(id)
    def message = s"node '$id' fills the input '$name', which the called flow does not have"
  }

  final case class MissingFlowInput(id: String, name: String) extends FlowError {
    val code = "missing-flow-input"
    val node: Option[String] = Some(id)
    def message = s"node '$id' does not fill the input '$name' of the called flow"
  }

  final case class UnknownFlowOutput(id: String, name: String) extends FlowError {
    val code = "unknown-flow-output"
    val node: Option[String] = Some(id)
    def message = s"node '$id' takes the output '$name', which the called flow does not expose"
  }

  final case class FlowOutputMismatch(id: String, name: String, expected: DataType, found: DataType) extends FlowError {
    val code = "flow-output-mismatch"
    val node: Option[String] = Some(id)
    def message = s"node '$id' takes '$name' as ${expected.render}, but the called flow exposes it as ${found.render}"
  }

  final case class OutputsWithoutWaiting(id: String) extends FlowError {
    val code = "outputs-without-waiting"
    val node: Option[String] = Some(id)
    def message = s"node '$id' does not wait for the called flow, so it cannot take outputs from it"
  }

  final case class UnknownInput(id: String, input: String, available: List[String]) extends FlowError {
    val code = "unknown-input"
    val node: Option[String] = Some(id)
    def message = s"node '$id' has no input '$input'; its inputs are ${available.mkString(", ")}"
  }

  /** An edge joins a stream port with a token one. A source or a sink must stand between them. */
  final case class StreamTokenMix(edge: Edge) extends FlowError {
    val code = "stream-token-mix"
    val node: Option[String] = Some(edge.to)
    def message = s"the edge ${edge.from} -> ${edge.to} joins a stream with an ordinary edge; a source or a sink must stand between them"
  }

  final case class StreamTypeMismatch(edge: Edge, expected: DataType, found: DataType) extends FlowError {
    val code = "stream-type-mismatch"
    val node: Option[String] = Some(edge.to)
    def message = s"the edge ${edge.from} -> ${edge.to} carries ${found.render}, but ${edge.to} expects ${expected.render}"
  }

  /** A group of nodes joined by streams that has no node where the stream begins. */
  final case class StreamWithoutSource(nodes: List[String]) extends FlowError {
    val code = "stream-without-source"
    val node: Option[String] = nodes.headOption
    def message = s"the stream among ${nodes.mkString(", ")} has no source"
  }

  /** A group of nodes joined by streams that has no node where the stream ends. */
  final case class StreamWithoutSink(nodes: List[String]) extends FlowError {
    val code = "stream-without-sink"
    val node: Option[String] = nodes.headOption
    def message = s"the stream among ${nodes.mkString(", ")} has no sink"
  }

  /** The flows that call each other in a circle, starting and ending with the one that is validated. */
  final case class FlowCycle(chain: List[String]) extends FlowError {
    val code = "flow-cycle"
    val node: Option[String] = None
    def message = s"flows call each other in a circle: ${chain.mkString(" -> ")}"
  }

  /** A type names an entity of the business model that does not exist. `place` says where: an input or an output of the flow,
    * or what a node reads or writes.
    */
  final case class UnknownEntity(id: Option[String], entity: String, place: String) extends FlowError {
    val code = "unknown-entity"
    val node: Option[String] = id
    def message = s"$place uses the entity '$entity', which does not exist"
  }

  implicit val encoder: Encoder[FlowError] = Encoder.instance { e =>
    val base = Json.obj(
      "code" -> Json.fromString(e.code),
      "node" -> e.node.fold(Json.Null)(Json.fromString),
      "message" -> Json.fromString(e.message)
    )
    // Problems that are about several nodes say which, so the editor can mark them.
    e match {
      case StreamWithoutSource(nodes) => base.mapObject(_.add("nodes", Json.fromValues(nodes.map(Json.fromString))))
      case StreamWithoutSink(nodes)   => base.mapObject(_.add("nodes", Json.fromValues(nodes.map(Json.fromString))))
      case Cycle(nodes)       => base.mapObject(_.add("nodes", Json.fromValues(nodes.map(Json.fromString))))
      case FlowCycle(chain)   => base.mapObject(_.add("chain", Json.fromValues(chain.map(Json.fromString))))
      case UnknownEntity(_, entity, _) => base.mapObject(_.add("entity", Json.fromString(entity)))
      case _                  => base
    }
  }
}
