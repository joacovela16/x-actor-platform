package xplat.flow.model

import io.circe.{Decoder, Encoder, Json, JsonObject}

/** A location in the global state of an execution, written with dots: `order.customer.id`. */
final case class StatePath(segments: List[String]) {

  def render: String = segments.mkString(".")

  def startsWith(prefix: StatePath): Boolean = segments.startsWith(prefix.segments)

  /** What is left of this path after `prefix`, which must be a prefix of it. */
  def dropPrefix(prefix: StatePath): StatePath = StatePath(segments.drop(prefix.segments.length))

  /** The value at this path in `state`, if there is one. */
  def get(state: Json): Option[Json] =
    segments.foldLeft(Option(state))((current, name) => current.flatMap(_.hcursor.downField(name).focus))

  /** `state` with `value` at this path. Missing objects are created; a value that is in the way is replaced. */
  def set(state: Json, value: Json): Json = segments match {
    case Nil => value
    case name :: rest =>
      val obj = state.asObject.getOrElse(JsonObject.empty)
      Json.fromJsonObject(obj.add(name, StatePath(rest).set(obj(name).getOrElse(Json.Null), value)))
  }

  override def toString: String = render
}

object StatePath {

  /** Reads `a.b.c`. No segment may be empty. */
  def parse(text: String): Either[String, StatePath] = {
    val segments = text.split("\\.", -1).toList
    if (text.isEmpty || segments.exists(_.isEmpty)) Left(s"not a valid state path: '$text'")
    else Right(StatePath(segments))
  }

  implicit val encoder: Encoder[StatePath] = Encoder.encodeString.contramap(_.render)
  implicit val decoder: Decoder[StatePath] = Decoder.decodeString.emap(parse)
}
