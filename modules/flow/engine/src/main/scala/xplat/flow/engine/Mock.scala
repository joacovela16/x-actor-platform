package xplat.flow.engine

import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import xplat.flow.model.Port

/** What a node answers in a simulation, instead of running. */
sealed trait Mock

object Mock {

  /** Answers with these values, by the name of the outputs of the node, through `port`. */
  final case class Output(values: Map[String, Json] = Map.empty, port: String = Port.Out) extends Mock

  /** Fails with this error. */
  final case class Fail(error: String) extends Mock

  implicit val encoder: Encoder[Mock] = Encoder.instance {
    case Output(values, port) => Json.obj("type" -> Json.fromString("output"), "values" -> Encoder[Map[String, Json]].apply(values), "port" -> Json.fromString(port))
    case Fail(error)          => Json.obj("type" -> Json.fromString("fail"), "error" -> Json.fromString(error))
  }

  implicit val decoder: Decoder[Mock] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case "output" =>
        for {
          values <- c.getOrElse[Map[String, Json]]("values")(Map.empty)
          port <- c.getOrElse[String]("port")(Port.Out)
        } yield Output(values, port)
      case "fail"  => c.get[String]("error").map(Fail(_))
      case other   => Left(DecodingFailure(s"unknown mock '$other'", c.history))
    }
  }
}
