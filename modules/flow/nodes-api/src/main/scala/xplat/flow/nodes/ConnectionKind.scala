package xplat.flow.nodes

import io.circe.{Encoder, Json}
import xplat.flow.model.{ConfigSchema, DataType}

/** A kind of global configuration that nodes refer to by name: what a connection of this kind needs, in a [[ConfigSchema]].
  * The fields marked `secret` go to the secret store and never come back.
  */
final case class ConnectionKind(kind: String, label: String, config: ConfigSchema)

object ConnectionKind {

  /** A key or a token, for example the one that a trigger checks. */
  val Secret: ConnectionKind = ConnectionKind(
    "secret",
    "Shared secret",
    ConfigSchema(List(ConfigSchema.Field("token", DataType.Str, secret = true, description = Some("The secret value"))))
  )

  /** An HTTP service, for the nodes that call one. */
  val Http: ConnectionKind = ConnectionKind(
    "http",
    "HTTP service",
    ConfigSchema(
      List(
        ConfigSchema.Field("baseUrl", DataType.Str, description = Some("Base URL, such as https://api.example.com")),
        ConfigSchema.Field("token", DataType.Str, required = false, secret = true, description = Some("Bearer token, if the service needs one"))
      )
    )
  )

  val defaults: List[ConnectionKind] = List(Secret, Http)

  implicit val encoder: Encoder[ConnectionKind] =
    Encoder.instance(k => Json.obj("kind" -> Json.fromString(k.kind), "label" -> Json.fromString(k.label), "config" -> Encoder[ConfigSchema].apply(k.config)))
}
