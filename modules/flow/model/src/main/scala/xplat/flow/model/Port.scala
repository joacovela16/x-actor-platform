package xplat.flow.model

import io.circe.{Decoder, Encoder}

/** An output (or, for [[NodeDescriptor.inputs]], an input) of a node that edges attach to. Nodes that branch pick one output
  * when they finish.
  *
  * @param stream
  *   the type of the elements, if the port carries a stream of them instead of handing the execution over once. An edge joins
  *   two ports of the same kind: stream with stream, token with token.
  * @param writes
  *   the outputs that a node that leaves through this port writes, when it is not all that it declares (a node that says a value
  *   is invalid writes the problems, not the value). The others are not written.
  */
final case class Port(name: String, label: Option[String] = None, stream: Option[DataType] = None, writes: Option[List[String]] = None)

object Port {

  /** The port of nodes that do not branch. */
  val Out: String = "out"

  /** The name of the input of nodes that have just one. */
  val In: String = "in"

  /** Every node has this port; a node that fails leaves through it. */
  val Error: String = "error"

  implicit val encoder: Encoder[Port] = Encoder
    .forProduct4("name", "label", "stream", "writes")((p: Port) => (p.name, p.label, p.stream, p.writes))
    .mapJsonObject(_.filter { case (_, v) => !v.isNull })
  implicit val decoder: Decoder[Port] = Decoder.forProduct4("name", "label", "stream", "writes")(Port.apply)
}
