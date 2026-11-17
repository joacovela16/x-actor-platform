package xplat.flow.model

import io.circe.{Decoder, DecodingFailure, Encoder}

/** What a kind of node does outside the execution. In test mode it decides what runs for real. */
sealed trait SideEffects

object SideEffects {

  /** Nothing outside the state. It always runs for real, also in test mode. */
  case object Pure extends SideEffects

  /** Reads from a system outside. */
  case object Read extends SideEffects

  /** Changes something outside. It is always simulated in test mode. */
  case object Write extends SideEffects

  implicit val encoder: Encoder[SideEffects] = Encoder.encodeString.contramap {
    case Pure  => "pure"
    case Read  => "read"
    case Write => "write"
  }
  implicit val decoder: Decoder[SideEffects] = Decoder.instance { c =>
    c.as[String].flatMap {
      case "pure"  => Right(Pure)
      case "read"  => Right(Read)
      case "write" => Right(Write)
      case other   => Left(DecodingFailure(s"unknown side effects '$other'", c.history))
    }
  }
}
