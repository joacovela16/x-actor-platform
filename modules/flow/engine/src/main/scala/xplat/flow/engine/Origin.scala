package xplat.flow.engine

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, DecodingFailure, Encoder, Json}

/** Who or what started an instance, or asked for it to run again. It is part of the audit trail. */
sealed trait Origin {

  /** A short name of the kind of origin, which instances can be searched by. */
  def kind: String

  /** The short form that the API shows: `manual`, `trigger:<id>`, `retry:<id>`, `subflow:<id>` or `cron:<flowId>/<nodeId>`. The detail (who, which
    * request, which node) is in the structured form.
    */
  def label: String = this match {
    case Origin.Manual(_)         => "manual"
    case Origin.Trigger(id, _)    => s"trigger:$id"
    case Origin.Retry(of, _)      => s"retry:$of"
    case Origin.Subflow(parent, _) => s"subflow:$parent"
    case Origin.Cron(schedule)    => s"cron:$schedule"
  }
}

object Origin {

  /** A person, or the API without more information. */
  final case class Manual(user: Option[String] = None) extends Origin { val kind = "manual" }

  /** A trigger, with the identifier of the request that it served when there is one. */
  final case class Trigger(triggerId: String, requestId: Option[String] = None) extends Origin { val kind = "trigger" }

  /** A node of another instance, which runs this one as a subflow. */
  final case class Subflow(parentInstanceId: String, parentNodeId: String) extends Origin { val kind = "subflow" }

  /** The schedule of a cron node of the flow (`<flowId>/<nodeId>`): the instance was started by the clock. */
  final case class Cron(schedule: String) extends Origin { val kind = "cron" }

  /** A retry of the instance `of`. */
  final case class Retry(of: String, by: Option[String] = None) extends Origin { val kind = "retry" }

  private def tag(o: Origin, json: Json): Json = json.mapObject(_.add("type", Json.fromString(o.kind)))

  implicit val encoder: Encoder[Origin] = Encoder.instance {
    case o: Manual  => tag(o, deriveEncoder[Manual].apply(o))
    case o: Trigger => tag(o, deriveEncoder[Trigger].apply(o))
    case o: Subflow => tag(o, deriveEncoder[Subflow].apply(o))
    case o: Retry   => tag(o, deriveEncoder[Retry].apply(o))
    case o: Cron    => tag(o, deriveEncoder[Cron].apply(o))
  }

  implicit val decoder: Decoder[Origin] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "manual"  => deriveDecoder[Manual].apply(c)
      case "trigger" => deriveDecoder[Trigger].apply(c)
      case "subflow" => deriveDecoder[Subflow].apply(c)
      case "retry"   => deriveDecoder[Retry].apply(c)
      case "cron"    => deriveDecoder[Cron].apply(c)
      case other     => Left(DecodingFailure(s"unknown origin '$other'", c.history))
    }
  }
}
