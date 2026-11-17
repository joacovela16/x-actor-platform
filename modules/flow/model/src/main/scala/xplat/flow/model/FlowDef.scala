package xplat.flow.model

import io.circe.{Decoder, Encoder}

/** The specification of a flow: a graph of nodes without cycles. It is plain data, stored and exchanged as JSON.
  *
  * @param version
  *   the version of the format, so flows that were saved can be migrated.
  * @param inputs
  *   the values in the global state when the execution starts. A trigger provides them, or the flow that calls this one.
  * @param outputs
  *   the values of its state that it exposes to the flows that call it, with their types. `target` is where they are in
  *   its own state.
  * @param revision
  *   the revision of the content, which grows with every change that is saved. Flows that call this one can name one.
  * @param layout
  *   where the editor draws each node, by node id. Optional, and ignored by everything but the editor.
  */
final case class FlowDef(
    id: String,
    name: String,
    nodes: List[NodeDef],
    edges: List[Edge] = Nil,
    inputs: List[OutputDecl] = Nil,
    outputs: List[OutputDecl] = Nil,
    revision: Int = 1,
    version: Int = FlowDef.CurrentVersion,
    layout: Map[String, Position] = Map.empty
)

object FlowDef {

  val CurrentVersion: Int = 1

  private val core: Encoder.AsObject[FlowDef] =
    Encoder.forProduct8("version", "id", "name", "revision", "inputs", "outputs", "nodes", "edges")((f: FlowDef) =>
      (f.version, f.id, f.name, f.revision, f.inputs, f.outputs, f.nodes, f.edges)
    )

  /** The layout is written only when there is one. */
  implicit val encoder: Encoder[FlowDef] = Encoder.instance { f =>
    val json = core(f)
    if (f.layout.isEmpty) json else json.mapObject(_.add("layout", Encoder[Map[String, Position]].apply(f.layout)))
  }

  implicit val decoder: Decoder[FlowDef] = Decoder.instance { c =>
    for {
      version <- c.getOrElse[Int]("version")(CurrentVersion)
      id <- c.get[String]("id")
      name <- c.get[String]("name")
      revision <- c.getOrElse[Int]("revision")(1)
      inputs <- c.getOrElse[List[OutputDecl]]("inputs")(Nil)
      outputs <- c.getOrElse[List[OutputDecl]]("outputs")(Nil)
      nodes <- c.get[List[NodeDef]]("nodes")
      edges <- c.getOrElse[List[Edge]]("edges")(Nil)
      layout <- c.getOrElse[Map[String, Position]]("layout")(Map.empty)
    } yield FlowDef(id, name, nodes, edges, inputs, outputs, revision, version, layout)
  }
}
