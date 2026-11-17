package xplat.flow.engine

import io.circe.{Encoder, Json}
import xplat.flow.engine.InstanceState.Status

/** The audit of one instance: what it is, and what happened in order. */
final case class AuditTrail(instance: AuditTrail.Header, entries: List[AuditEntry])

object AuditTrail {

  /** @param flowHash
    *   a fingerprint of the definition that ran, so two instances can be told to have run the same one.
    */
  final case class Header(
      id: InstanceId,
      flowId: String,
      revision: Int,
      flowHash: String,
      status: Status,
      origin: Origin,
      inputs: Json,
      startedAt: Long,
      endedAt: Option[Long]
  )

  /** `origin` is the short label (`trigger:orders`) and `originDetail` the structured one. */
  implicit val headerEncoder: Encoder[Header] = Encoder.instance { h =>
    Json
      .obj(
        "id" -> Json.fromString(h.id),
        "flowId" -> Json.fromString(h.flowId),
        "revision" -> Json.fromInt(h.revision),
        "flowHash" -> Json.fromString(h.flowHash),
        "status" -> Encoder[Status].apply(h.status),
        "origin" -> Json.fromString(h.origin.label),
        "originDetail" -> Encoder[Origin].apply(h.origin),
        "inputs" -> h.inputs,
        "startedAt" -> Json.fromLong(h.startedAt),
        "endedAt" -> Encoder[Option[Long]].apply(h.endedAt)
      )
      .mapObject(_.filter { case (_, v) => !v.isNull })
  }

  implicit val encoder: Encoder[AuditTrail] = Encoder.forProduct2("instance", "entries")((t: AuditTrail) => (t.instance, t.entries))
}
