package xplat.flow.model

import io.circe.{Decoder, DecodingFailure, Encoder}

/** When a node with several incoming edges runs. An edge that will never fire, because a branch was not taken, counts as
  * arrived for [[JoinPolicy.AllInputs]], so the node does not wait for it.
  */
sealed trait JoinPolicy

object JoinPolicy {

  /** Waits for every incoming edge. */
  case object AllInputs extends JoinPolicy

  /** Runs with the first edge that arrives; the others are discarded. */
  case object AnyInput extends JoinPolicy

  implicit val encoder: Encoder[JoinPolicy] = Encoder.encodeString.contramap {
    case AllInputs => "all"
    case AnyInput  => "any"
  }
  implicit val decoder: Decoder[JoinPolicy] = Decoder.instance { c =>
    c.as[String].flatMap {
      case "all" => Right(AllInputs)
      case "any" => Right(AnyInput)
      case other => Left(DecodingFailure(s"unknown join policy '$other'", c.history))
    }
  }
}
