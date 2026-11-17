package xplat.flow.model

import io.circe.{Decoder, Encoder, Json}

/** What a node receives when it runs.
  *
  * @param params
  *   its configuration.
  * @param state
  *   the global state of the execution, shared by every node.
  * @param memory
  *   the memory of this node in this execution. No other node sees it.
  * @param instanceId
  *   the instance that runs the node, and `nodeId` the node in its flow. For nodes that need to say where they come from,
  *   such as one that starts another flow. Empty when the node is run on its own.
  * @param now
  *   the time, in milliseconds since the epoch. A node that needs the time asks for it here and not from the clock, so a
  *   simulation, which has a time of its own, can run it.
  * @param entry
  *   the node has no incoming edge: the execution starts in it.
  * @param firedInputs
  *   the inputs (see [[Edge.input]]) through which an edge fired. A stream segment, which has several sources, uses it to
  *   know which of them to start.
  * @param outputs
  *   what the node declared that it writes. The engine accepts exactly these names in the answer, so a node that can answer
  *   several values (`rows`, `count`) uses it to answer only the ones that were asked for. Empty when the node is run on its own.
  */
final case class NodeInput(
    params: Json,
    state: Json,
    memory: Json = Json.obj(),
    instanceId: String = "",
    nodeId: String = "",
    now: Long = 0L,
    entry: Boolean = false,
    firedInputs: List[String] = Nil,
    outputs: List[OutputDecl] = Nil
)

object NodeInput {
  implicit val encoder: Encoder[NodeInput] =
    Encoder.forProduct9("params", "state", "memory", "instanceId", "nodeId", "now", "entry", "firedInputs", "outputs")((i: NodeInput) =>
      (i.params, i.state, i.memory, i.instanceId, i.nodeId, i.now, i.entry, i.firedInputs, i.outputs)
    )
  implicit val decoder: Decoder[NodeInput] = Decoder.instance { c =>
    for {
      params <- c.get[Json]("params")
      state <- c.get[Json]("state")
      memory <- c.getOrElse[Json]("memory")(Json.obj())
      instanceId <- c.getOrElse[String]("instanceId")("")
      nodeId <- c.getOrElse[String]("nodeId")("")
      now <- c.getOrElse[Long]("now")(0L)
      entry <- c.getOrElse[Boolean]("entry")(false)
      fired <- c.getOrElse[List[String]]("firedInputs")(Nil)
      outputs <- c.getOrElse[List[OutputDecl]]("outputs")(Nil)
    } yield NodeInput(params, state, memory, instanceId, nodeId, now, entry, fired, outputs)
  }
}
