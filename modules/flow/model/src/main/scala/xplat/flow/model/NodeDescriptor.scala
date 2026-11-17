package xplat.flow.model

import io.circe.Json

/** What is known about a kind of node without running it: enough to validate a flow and to draw the node in the editor.
  * The code that runs it lives apart, so this is available in the browser too.
  *
  * Everything that takes `params` receives the configuration of one node of this kind, and must not fail for any value.
  *
  * @param kind
  *   unique name, such as `core.if`.
  * @param category
  *   how the editor groups the kind in its palette. The engine ignores it.
  * @param config
  *   the fields of its configuration.
  * @param ports
  *   where edges leave from, apart from [[Port.Error]], which every node has.
  * @param defaultOutputs
  *   what a new node of this kind writes in the state, which the user can change.
  * @param reads
  *   what the node needs to find in the state.
  * @param checkParams
  *   problems of the configuration that the schema cannot see.
  * @param calls
  *   the flow that a node of this kind runs, if it runs one. The validator checks it against the called flow.
  * @param inputs
  *   where edges enter. Most nodes have one, `in`. A port with a `stream` type receives a stream.
  * @param description
  *   what the node does, in a sentence or two, for the editor to show.
  */
final case class NodeDescriptor(
    kind: String,
    category: String,
    label: String,
    config: ConfigSchema = ConfigSchema.empty,
    sideEffects: SideEffects = SideEffects.Pure,
    ports: Json => List[Port] = _ => List(Port(Port.Out)),
    defaultOutputs: Json => List[OutputDecl] = _ => Nil,
    reads: Json => List[StateRead] = _ => Nil,
    checkParams: Json => List[String] = _ => Nil,
    calls: Json => Option[FlowCall] = _ => None,
    inputs: Json => List[Port] = _ => List(Port(Port.In)),
    description: String = ""
) {

  /** The names of every port that an edge can leave from. */
  def portNames(params: Json): List[String] = (ports(params).map(_.name) :+ Port.Error).distinct

  /** The type of the elements that leave through `port`, if it is a stream. */
  def streamOut(params: Json, port: String): Option[DataType] = ports(params).find(_.name == port).flatMap(_.stream)

  /** The type of the elements that enter through `input`, if it is a stream. */
  def streamIn(params: Json, input: String): Option[DataType] = inputs(params).find(_.name == input).flatMap(_.stream)

  /** Whether a node of this kind takes part in a stream: some of its inputs or outputs carry one. */
  def isStream(params: Json): Boolean = ports(params).exists(_.stream.isDefined) || inputs(params).exists(_.stream.isDefined)

  /** A node where streams begin: it has stream outputs and no stream input (it is started by an ordinary edge). */
  def isSource(params: Json): Boolean = isStream(params) && !inputs(params).exists(_.stream.isDefined)

  /** A node where streams end: it has stream inputs and no stream output (it hands the execution on when the stream ends). */
  def isSink(params: Json): Boolean = isStream(params) && !ports(params).exists(_.stream.isDefined)
}
