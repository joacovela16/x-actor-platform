package xplat.flow.model

import io.circe.{Decoder, Encoder, Json}

/** A connection from a port of a node to an input of another node. Every edge leaving a port fires when the node leaves
  * through it, so a port with several edges broadcasts. If the port carries a stream, the elements go to every edge, and the
  * slowest consumer sets the pace.
  *
  * @param input
  *   the input of `to` that the edge enters, for the nodes that have several (`left` and `right` of a join). In JSON it is
  *   left out when it is `in`.
  */
final case class Edge(from: String, to: String, port: String = Port.Out, input: String = Port.In)

object Edge {
  implicit val encoder: Encoder[Edge] = Encoder.instance { e =>
    val base = Json.obj("from" -> Json.fromString(e.from), "port" -> Json.fromString(e.port), "to" -> Json.fromString(e.to))
    if (e.input == Port.In) base else base.mapObject(_.add("input", Json.fromString(e.input)))
  }
  implicit val decoder: Decoder[Edge] = Decoder.instance { c =>
    for {
      from <- c.get[String]("from")
      to <- c.get[String]("to")
      port <- c.getOrElse[String]("port")(Port.Out)
      input <- c.getOrElse[String]("input")(Port.In)
    } yield Edge(from, to, port, input)
  }
}
