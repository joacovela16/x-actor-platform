package xplat.flow.nodes

import cats.Applicative
import cats.syntax.applicative._
import io.circe.{Decoder, Json}
import xplat.flow.model._

/** Ends the part of a flow that a caller is waiting for. When a trigger answers synchronously, the response of the HTTP
  * request is the one that the first of these nodes to finish describes: the status, the headers and, as the body, the value
  * at `body` of the global state. The flow may go on after it.
  *
  * The node itself only checks that the body is there; the HTTP layer builds the response from the state, so no secret has to
  * pass through the node.
  */
final class RespondNode[F[_]: Applicative] extends NodeType[F] {

  val descriptor: NodeDescriptor = RespondNode.descriptor

  def run(input: NodeInput): F[NodeOutput] = RespondNode.evaluate(input).pure[F]
}

object RespondNode {

  val Kind: String = "core.respond"

  /** @param status
    *   the HTTP status.
    * @param body
    *   where in the state the body is; the response has no body if it is empty.
    */
  final case class Params(status: Int = 200, body: Option[StatePath] = None, headers: Map[String, String] = Map.empty)

  object Params {
    implicit val decoder: Decoder[Params] = Decoder.instance { c =>
      for {
        status <- c.getOrElse[Int]("status")(200)
        body <- c.get[Option[StatePath]]("body")
        headers <- c.getOrElse[Map[String, String]]("headers")(Map.empty)
      } yield Params(status, body, headers)
    }
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Triggers",
    label = "Respond",
    description = "Answers the caller that is waiting for the flow (a synchronous trigger): status, headers and a value of the state as the body. The flow can go on after it.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("status", DataType.Num, required = false, default = Some(Json.fromInt(200)), description = Some("HTTP status of the response")),
        ConfigSchema.Field("body", DataType.Str, required = false, description = Some("Path in the state of the body, such as result.order")),
        ConfigSchema.Field("headers", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Headers of the response, by name"))
      )
    ),
    sideEffects = SideEffects.Pure,
    reads = params => params.as[Params].toOption.flatMap(_.body).map(StateRead(_, DataType.AnyJson)).toList,
    checkParams = params =>
      params.as[Params] match {
        case Left(e)                                    => List(e.message)
        case Right(p) if p.status < 100 || p.status > 599 => List(s"status ${p.status} is not an HTTP status")
        case Right(_)                                   => Nil
      }
  )

  def evaluate(input: NodeInput): NodeOutput =
    input.params.as[Params] match {
      case Left(error) => NodeOutput.failed(s"invalid configuration: ${error.message}")
      case Right(p) =>
        p.body.filter(_.get(input.state).isEmpty) match {
          case Some(missing) => NodeOutput.failed(s"the body '$missing' is not in the state")
          case None          => NodeOutput.ok()
        }
    }
}
