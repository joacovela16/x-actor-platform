package xplat.flow.model

import io.circe.{Decoder, Encoder}

/** A value that a node writes in the global state: what it is called, where it goes and its type. */
final case class OutputDecl(name: String, target: StatePath, tpe: DataType)

object OutputDecl {
  implicit val encoder: Encoder[OutputDecl] = Encoder.forProduct3("name", "target", "type")(o => (o.name, o.target, o.tpe))
  implicit val decoder: Decoder[OutputDecl] = Decoder.forProduct3("name", "target", "type")(OutputDecl.apply)
}
