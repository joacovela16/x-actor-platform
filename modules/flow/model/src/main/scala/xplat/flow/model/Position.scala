package xplat.flow.model

import io.circe.{Decoder, Encoder}

/** Where the editor draws a node. It is only for the editor: the engine and the validator ignore it. */
final case class Position(x: Double, y: Double)

object Position {
  implicit val encoder: Encoder[Position] = Encoder.forProduct2("x", "y")((p: Position) => (p.x, p.y))
  implicit val decoder: Decoder[Position] = Decoder.forProduct2("x", "y")(Position.apply)
}
