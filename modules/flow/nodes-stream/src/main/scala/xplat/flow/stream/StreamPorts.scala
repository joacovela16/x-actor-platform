package xplat.flow.stream

import io.circe.Json
import xplat.flow.model.{DataType, Expr, Port, StatePath}

import scala.concurrent.duration._

/** The ports and small pieces that the stream nodes have in common. */
object StreamPorts {

  val SourceCategory: String = "Stream sources"
  val OperatorCategory: String = "Stream operators"
  val SinkCategory: String = "Stream sinks"

  /** One stream of anything leaves through `out`. */
  def output(tpe: DataType = DataType.AnyJson): List[Port] = List(Port(Port.Out, stream = Some(tpe)))

  /** One stream of anything enters through `in`. */
  val input: List[Port] = List(Port(Port.In, stream = Some(DataType.AnyJson)))

  /** Two streams enter, through `left` and `right`. */
  val sides: List[Port] = List(Port("left", stream = Some(DataType.AnyJson)), Port("right", stream = Some(DataType.AnyJson)))

  /** A length of time written as text (`5 seconds`, `200 millis`). It must be finite and positive. */
  def duration(text: String): Either[String, FiniteDuration] =
    scala.util.Try(Duration(text)).toOption match {
      case Some(d: FiniteDuration) if d > Duration.Zero => Right(d)
      case _                                            => Left(s"'$text' is not a length of time such as '5 seconds'")
    }

  def path(text: String): Either[String, StatePath] = StatePath.parse(text)

  def text(json: Json, field: String): Option[String] = json.hcursor.get[String](field).toOption.filter(_.nonEmpty)

  /** A whole number that is 0 or more, at `field`. */
  def count(json: Json, field: String): Either[String, Int] =
    json.hcursor.get[Int](field).left.map(_ => s"missing field '$field', a whole number").filterOrElse(_ >= 0, s"$field cannot be negative")

  /** The condition `where` (see [[Expr]]). */
  def condition(json: Json): Either[String, Expr] =
    json.hcursor.get[Json]("where").left.map(_ => "missing field 'where'").flatMap(Expr.parse(_).left.map(m => s"where: $m"))

  /** Whether `where` is true for the element, or why it cannot be said. */
  def holds[F[_]](where: Expr, e: Elem[F])(implicit F: cats.Applicative[F]): F[Either[String, Boolean]] =
    F.pure(where.eval(e.value).flatMap(v => v.asBoolean.toRight(s"the condition is not true or false: ${v.noSpaces}")))

  /** The optional `key`: a path in the element, or none for the whole element. */
  def key(json: Json): Either[String, Option[StatePath]] =
    json.hcursor.get[Option[String]]("key").left.map(_.message).flatMap(k => k.filter(_.nonEmpty) match {
      case None    => Right(None)
      case Some(t) => StatePath.parse(t).map(Some(_))
    })

  /** The key of an element: the value at `key`, or the whole element. */
  def keyOf(key: Option[StatePath], value: Json): Either[String, Json] =
    key.fold[Either[String, Json]](Right(value))(k => k.get(value).toRight(s"'${k.render}' is not in the element"))
}
