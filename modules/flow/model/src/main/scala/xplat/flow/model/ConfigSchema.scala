package xplat.flow.model

import io.circe.{Decoder, Encoder, Json}

/** The fields that a kind of node (or a connection) accepts in its configuration. It checks the configuration on the
  * server and describes the form that the editor shows.
  */
final case class ConfigSchema(fields: List[ConfigSchema.Field]) {

  /** The problems of `params`, empty when it is valid. A field that is missing is fine if it has a default. `types` knows the
    * entities that a field may be typed with.
    */
  def validate(params: Json, types: Types = Types.empty): List[String] = {
    val obj = if (params.isNull) Some(io.circe.JsonObject.empty) else params.asObject
    obj match {
      case None => List("the configuration must be an object")
      case Some(o) =>
        val known = fields.map(_.name).toSet
        val unknown = o.keys.filterNot(known).toList.sorted.map(name => s"unknown field '$name'")
        val own = fields.flatMap { field =>
          o(field.name) match {
            case None if field.required && field.default.isEmpty => List(s"missing field '${field.name}'")
            case None                                           => Nil
            case Some(v) if !field.tpe.accepts(v, types)        => List(s"field '${field.name}' must be ${field.tpe.render}")
            case Some(v) if field.oneOf.nonEmpty && !field.oneOf.contains(v) =>
              List(s"field '${field.name}' must be one of ${field.oneOf.map(_.noSpaces).mkString(", ")}")
            case Some(_) => Nil
          }
        }
        own ++ unknown
    }
  }
}

object ConfigSchema {

  val empty: ConfigSchema = ConfigSchema(Nil)

  /** @param secret
    *   the value is a credential: it is kept apart from the specification and never sent to the editor.
    * @param oneOf
    *   the values allowed, when they are a fixed list.
    */
  final case class Field(
      name: String,
      tpe: DataType,
      required: Boolean = true,
      default: Option[Json] = None,
      secret: Boolean = false,
      oneOf: List[Json] = Nil,
      description: Option[String] = None,
      ui: Option[UiSpec] = None
  )

  object Field {
    implicit val encoder: Encoder[Field] =
      Encoder
        .forProduct8("name", "type", "required", "default", "secret", "oneOf", "description", "ui")((f: Field) =>
          (f.name, f.tpe, f.required, f.default, f.secret, f.oneOf, f.description, f.ui)
        )
        .mapJsonObject(_.filter { case (_, v) => !v.isNull })

    implicit val decoder: Decoder[Field] = Decoder.instance { c =>
      for {
        name <- c.get[String]("name")
        tpe <- c.get[DataType]("type")
        required <- c.getOrElse[Boolean]("required")(true)
        default <- c.get[Option[Json]]("default")
        secret <- c.getOrElse[Boolean]("secret")(false)
        oneOf <- c.getOrElse[List[Json]]("oneOf")(Nil)
        description <- c.get[Option[String]]("description")
        ui <- c.get[Option[UiSpec]]("ui")
      } yield Field(name, tpe, required, default, secret, oneOf, description, ui)
    }
  }

  implicit val encoder: Encoder[ConfigSchema] = Encoder.forProduct1("fields")((s: ConfigSchema) => s.fields)
  implicit val decoder: Decoder[ConfigSchema] = Decoder.forProduct1("fields")(ConfigSchema.apply)
}
