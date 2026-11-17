package xplat.flow.model

import io.circe.{Decoder, Encoder, Json}

/** How the editor draws a field of a [[ConfigSchema]]. It is data, not code: the editor has one interpreter that renders any
  * schema, so a new kind of node or connection needs no editor of its own. Without it the editor infers a widget from the type
  * of the field.
  *
  * @param widget
  *   one of [[UiSpec.Widget]]'s names: text, textarea, code, password, number, checkbox, select, json, entity (the name of an
  *   entity of the business model).
  * @param language
  *   for `code`: javascript, scala, sql, json.
  * @param visibleWhen
  *   the field is shown only when another field of the same configuration has this value.
  * @param group
  *   fields with the same group are drawn together under its title.
  * @param template
  *   the value a new node starts with, such as example code for `code`.
  * @param filesField
  *   for `code`: the field (a list of `{name, code}`) with more files of the same code; the editor shows them as tabs of this one, and
  *   does not draw that field on its own.
  */
final case class UiSpec(
    widget: Option[String] = None,
    language: Option[String] = None,
    rows: Option[Int] = None,
    placeholder: Option[String] = None,
    help: Option[String] = None,
    options: List[UiSpec.Choice] = Nil,
    visibleWhen: Option[UiSpec.VisibleWhen] = None,
    group: Option[String] = None,
    order: Option[Int] = None,
    template: Option[String] = None,
    filesField: Option[String] = None
)

object UiSpec {

  final case class Choice(value: Json, label: String)
  final case class VisibleWhen(field: String, equals: Json)

  val empty: UiSpec = UiSpec()

  def code(language: String, rows: Int = 12): UiSpec = UiSpec(widget = Some("code"), language = Some(language), rows = Some(rows))

  /** Code that starts as `template` and can have more files, kept in the field `filesField`. */
  def script(language: String, template: String, filesField: String, rows: Int = 16): UiSpec =
    code(language, rows).copy(template = Some(template), filesField = Some(filesField))
  def textarea(rows: Int = 5): UiSpec = UiSpec(widget = Some("textarea"), rows = Some(rows))
  def password: UiSpec = UiSpec(widget = Some("password"))
  def entity: UiSpec = UiSpec(widget = Some("entity"))

  implicit val choiceEncoder: Encoder[Choice] = Encoder.forProduct2("value", "label")((o: Choice) => (o.value, o.label))
  implicit val choiceDecoder: Decoder[Choice] = Decoder.forProduct2("value", "label")(Choice.apply)
  implicit val visibleEncoder: Encoder[VisibleWhen] = Encoder.forProduct2("field", "equals")((v: VisibleWhen) => (v.field, v.equals))
  implicit val visibleDecoder: Decoder[VisibleWhen] = Decoder.forProduct2("field", "equals")(VisibleWhen.apply)

  implicit val encoder: Encoder[UiSpec] =
    Encoder
      .forProduct11("widget", "language", "rows", "placeholder", "help", "options", "visibleWhen", "group", "order", "template", "filesField")(
        (u: UiSpec) => (u.widget, u.language, u.rows, u.placeholder, u.help, u.options, u.visibleWhen, u.group, u.order, u.template, u.filesField)
      )
      .mapJsonObject(_.filter { case (_, v) => !v.isNull && v != Json.arr() })

  implicit val decoder: Decoder[UiSpec] = Decoder.instance { c =>
    for {
      widget <- c.get[Option[String]]("widget")
      language <- c.get[Option[String]]("language")
      rows <- c.get[Option[Int]]("rows")
      placeholder <- c.get[Option[String]]("placeholder")
      help <- c.get[Option[String]]("help")
      options <- c.getOrElse[List[Choice]]("options")(Nil)
      visibleWhen <- c.get[Option[VisibleWhen]]("visibleWhen")
      group <- c.get[Option[String]]("group")
      order <- c.get[Option[Int]]("order")
      template <- c.get[Option[String]]("template")
      filesField <- c.get[Option[String]]("filesField")
    } yield UiSpec(widget, language, rows, placeholder, help, options, visibleWhen, group, order, template, filesField)
  }
}
