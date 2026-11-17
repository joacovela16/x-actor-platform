package xplat.flow.db

import io.circe.{Decoder, DecodingFailure, Json}
import xplat.flow.model.{NodeInput, StatePath}

/** Where the value that goes into a `?` of a statement comes from. In the configuration it is written as
  *
  *  - `{"path": "order.id"}`: a value of the global state;
  *  - `{"value": 3}`: a fixed value;
  *  - `{"context": "key"}`: what identifies this run: `instanceId`, `nodeId` or `key` (`<instanceId>:<nodeId>`), to make a
  *    write that a retry does not repeat (a unique column plus `INSERT ... ON CONFLICT DO NOTHING`);
  *  - or a plain string, number, boolean or `null`, which is a fixed value.
  *
  * The value is always bound as a parameter of the statement, never written into its text.
  */
sealed trait ParamSpec {

  /** The value, or why it cannot be had. */
  def resolve(input: NodeInput): Either[String, Json]
}

object ParamSpec {

  final case class FromState(path: StatePath) extends ParamSpec {
    def resolve(input: NodeInput): Either[String, Json] = path.get(input.state).toRight(s"'$path' is not in the state")
  }

  final case class Literal(value: Json) extends ParamSpec {
    def resolve(input: NodeInput): Either[String, Json] = Right(value)
  }

  final case class FromContext(name: String) extends ParamSpec {
    def resolve(input: NodeInput): Either[String, Json] = name match {
      case "instanceId" => Right(Json.fromString(input.instanceId))
      case "nodeId"     => Right(Json.fromString(input.nodeId))
      case _            => Right(Json.fromString(s"${input.instanceId}:${input.nodeId}"))
    }
  }

  val ContextNames: Set[String] = Set("instanceId", "nodeId", "key")

  implicit val decoder: Decoder[ParamSpec] = Decoder.instance { c =>
    c.value.asObject match {
      case None => Right(Literal(c.value))
      case Some(o) =>
        (o("path"), o("value"), o("context")) match {
          case (Some(p), None, None) =>
            p.as[StatePath].map(FromState(_)).left.map(e => DecodingFailure(s"path: ${e.message}", c.history))
          case (None, Some(v), None) => Right(Literal(v))
          case (None, None, Some(n)) =>
            n.asString.filter(ContextNames).map(FromContext(_)).toRight(DecodingFailure(s"context must be one of ${ContextNames.toList.sorted.mkString(", ")}", c.history))
          case _ => Left(DecodingFailure("a parameter is {\"path\": ...}, {\"value\": ...} or {\"context\": ...}, with only one of them", c.history))
        }
    }
  }

  /** The values of all of them, or the first problem. */
  def resolveAll(specs: List[ParamSpec], input: NodeInput): Either[String, List[Json]] =
    specs.foldLeft[Either[String, List[Json]]](Right(Nil))((acc, s) => acc.flatMap(done => s.resolve(input).map(done :+ _)))

  /** The paths of the state that these read. */
  def reads(specs: List[ParamSpec]): List[StatePath] = specs.collect { case FromState(p) => p }
}
