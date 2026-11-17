package xplat.flow.engine

import io.circe.{Decoder, Encoder, Json}
import xplat.flow.engine.InstanceState.Status

/** An instance in a list. Times are milliseconds since the epoch. */
final case class InstanceSummary(
    id: InstanceId,
    flowId: String,
    revision: Int,
    status: Status,
    origin: Origin,
    startedAt: Long,
    endedAt: Option[Long]
)

object InstanceSummary {

  def of(s: InstanceState): Option[InstanceSummary] =
    s.flow.map(f => InstanceSummary(s.id, f.id, f.revision, s.status, s.origin, s.startedAt, s.endedAt))

  /** `origin` is the short label (`trigger:orders`) and `originDetail` the structured one. */
  implicit val encoder: Encoder[InstanceSummary] = Encoder.instance { i =>
    Json
      .obj(
        "id" -> Json.fromString(i.id),
        "flowId" -> Json.fromString(i.flowId),
        "revision" -> Json.fromInt(i.revision),
        "status" -> Encoder[Status].apply(i.status),
        "origin" -> Json.fromString(i.origin.label),
        "originDetail" -> Encoder[Origin].apply(i.origin),
        "startedAt" -> Json.fromLong(i.startedAt),
        "endedAt" -> Encoder[Option[Long]].apply(i.endedAt)
      )
      .mapObject(_.filter { case (_, v) => !v.isNull })
  }

  implicit val decoder: Decoder[InstanceSummary] = Decoder.instance { c =>
    for {
      id <- c.get[String]("id")
      flowId <- c.get[String]("flowId")
      revision <- c.get[Int]("revision")
      status <- c.get[Status]("status")
      origin <- c.get[Origin]("originDetail")
      startedAt <- c.get[Long]("startedAt")
      endedAt <- c.get[Option[Long]]("endedAt")
    } yield InstanceSummary(id, flowId, revision, status, origin, startedAt, endedAt)
  }
}
