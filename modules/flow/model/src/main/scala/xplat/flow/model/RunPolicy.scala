package xplat.flow.model

import io.circe.{Decoder, DecodingFailure, Encoder}

/** What a node does when its instance is run again or retried, depending on how it went the last time.
  *
  * A node that never ran always runs. With [[RunPolicy.Skip]] the node does not run: the execution goes on with the
  * result it had, and the values it wrote in the state are applied again.
  *
  * @param whenSucceeded
  *   what to do if the last run succeeded.
  * @param whenFailed
  *   what to do if the last run failed.
  */
final case class RunPolicy(
    whenSucceeded: RunPolicy.Action = RunPolicy.Run,
    whenFailed: RunPolicy.Action = RunPolicy.Run
)

object RunPolicy {
  sealed trait Action
  case object Run extends Action
  case object Skip extends Action

  object Action {
    implicit val encoder: Encoder[Action] = Encoder.encodeString.contramap {
      case Run  => "run"
      case Skip => "skip"
    }
    implicit val decoder: Decoder[Action] = Decoder.instance { c =>
      c.as[String].flatMap {
        case "run"  => Right(Run)
        case "skip" => Right(Skip)
        case other  => Left(DecodingFailure(s"unknown action '$other'", c.history))
      }
    }
  }

  /** Runs every time. */
  val Always: RunPolicy = RunPolicy()

  /** Does not run again once it succeeded. */
  val OnceSucceeded: RunPolicy = RunPolicy(whenSucceeded = Skip)

  implicit val encoder: Encoder[RunPolicy] = Encoder.forProduct2("whenSucceeded", "whenFailed")(p => (p.whenSucceeded, p.whenFailed))
  implicit val decoder: Decoder[RunPolicy] = Decoder.instance { c =>
    for {
      succeeded <- c.getOrElse[Action]("whenSucceeded")(Run)
      failed <- c.getOrElse[Action]("whenFailed")(Run)
    } yield RunPolicy(succeeded, failed)
  }
}
