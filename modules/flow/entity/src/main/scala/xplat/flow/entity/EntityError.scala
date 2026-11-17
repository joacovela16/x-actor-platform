package xplat.flow.entity

import io.circe.syntax._
import io.circe.{Encoder, Json}

/** A problem that makes the definition of an entity invalid. The validator reports all of them at once.
  *
  * @param code
  *   a short stable name, for the editor to tell the kinds of problem apart.
  * @param field
  *   the field the problem is about, if it is about one.
  */
final case class EntityError(code: String, entity: String, field: Option[String], message: String)

object EntityError {

  def apply(code: String, entity: String, message: String): EntityError = EntityError(code, entity, None, message)

  def onField(code: String, entity: String, field: String, message: String): EntityError = EntityError(code, entity, Some(field), message)

  implicit val encoder: Encoder[EntityError] = Encoder.instance { e =>
    Json.obj("code" -> e.code.asJson, "entity" -> e.entity.asJson, "field" -> e.field.asJson, "message" -> e.message.asJson)
  }
}
