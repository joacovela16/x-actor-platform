package xplat.flow.model

import io.circe.{Decoder, Encoder, Json}

/** A node of a flow.
  *
  * @param kind
  *   the kind of node, as named in the [[NodeCatalog]].
  * @param params
  *   its configuration, as its [[ConfigSchema]] describes it.
  * @param outputs
  *   what it writes in the global state. It writes nothing else.
  */
final case class NodeDef(
    id: String,
    kind: String,
    params: Json = Json.obj(),
    outputs: List[OutputDecl] = Nil,
    runPolicy: RunPolicy = RunPolicy.Always,
    joinPolicy: JoinPolicy = JoinPolicy.AllInputs,
    label: Option[String] = None
)

object NodeDef {
  implicit val encoder: Encoder[NodeDef] =
    Encoder
      .forProduct7("id", "kind", "params", "outputs", "runPolicy", "joinPolicy", "label")((n: NodeDef) =>
        (n.id, n.kind, n.params, n.outputs, n.runPolicy, n.joinPolicy, n.label)
      )
      .mapJsonObject(_.filter { case (_, v) => !v.isNull })

  implicit val decoder: Decoder[NodeDef] = Decoder.instance { c =>
    for {
      id <- c.get[String]("id")
      kind <- c.get[String]("kind")
      params <- c.getOrElse[Json]("params")(Json.obj())
      outputs <- c.getOrElse[List[OutputDecl]]("outputs")(Nil)
      runPolicy <- c.getOrElse[RunPolicy]("runPolicy")(RunPolicy.Always)
      joinPolicy <- c.getOrElse[JoinPolicy]("joinPolicy")(JoinPolicy.AllInputs)
      label <- c.get[Option[String]]("label")
    } yield NodeDef(id, kind, params, outputs, runPolicy, joinPolicy, label)
  }
}
