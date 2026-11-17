package xplat.flow.queue

import cats.MonadThrow
import cats.syntax.all._
import io.circe.{Decoder, Json}
import xplat.core.connectors.Outgoing
import xplat.flow.model._
import xplat.flow.nodes.{Connections, NodeType, Traces}

import java.nio.charset.StandardCharsets.UTF_8

/** Publishes a value of the state to a topic, queue or stream of a broker (Kafka, ActiveMQ, Redis Streams).
  *
  * Every message carries the header `x-idempotency-key` = `<instanceId>:<nodeId>`, so a consumer can drop the duplicate that
  * a retry produces (delivery is at least once). With `RunPolicy(whenSucceeded = skip)` a retried instance does not
  * publish again at all. The connection is a named one (see [[QueueKinds]]); its secrets are never in the audit.
  *
  * It writes outside the instance, so in a simulation it does not connect: it answers with the message it would have sent
  * (in the `info` of the audit entry).
  */
final class PublishNode[F[_]](connections: Connections[F], pool: PublisherPool[F], traces: Option[Traces[F]] = None)(implicit F: MonadThrow[F]) extends NodeType[F] {

  val descriptor: NodeDescriptor = PublishNode.descriptor

  def run(input: NodeInput): F[NodeOutput] =
    PublishNode.params(input.params).flatMap(p => PublishNode.build(p, input).map(p -> _)) match {
      case Left(problem) => NodeOutput.failed(problem).pure[F]
      case Right((params, message)) =>
        connections.resolve(params.connection).flatMap {
          case None => NodeOutput.failed(s"the connection '${params.connection}' does not exist").pure[F]
          case Some(connection) =>
            // The message carries the `traceparent` of this node, unless its headers set one.
            val traceparent =
              if (message.headers.keys.exists(_.equalsIgnoreCase(Traces.Header))) F.pure(Option.empty[String])
              else traces.fold(F.pure(Option.empty[String]))(_.traceparent(input.instanceId, input.nodeId))
            traceparent.flatMap { t =>
            pool
              .use(connection, params.destination, params.destinationType)(_.publish(Outgoing(message.bytes, message.key, message.headers ++ t.map(Traces.Header -> _))))
              .map(_ => NodeOutput(info = PublishNode.info(params, message, withPayload = false)))
              .handleError(e => NodeOutput.failed(s"cannot publish to ${params.destination}: ${Option(e.getMessage).getOrElse(e.toString)}"))
            }
        }
    }

  override def simulated(input: NodeInput): Option[NodeOutput] =
    Some(PublishNode.params(input.params).flatMap(p => PublishNode.build(p, input).map(p -> _)) match {
      case Left(problem)            => NodeOutput.failed(problem)
      case Right((params, message)) => NodeOutput(info = PublishNode.info(params, message, withPayload = true))
    })
}

object PublishNode {

  val Kind: String = "queue.publish"
  val IdempotencyHeader: String = "x-idempotency-key"

  /** @param connection
    *   name of the connection.
    * @param destination
    *   the topic (Kafka), queue or topic (ActiveMQ) or stream (Redis).
    * @param payload
    *   path in the state of the value to publish.
    * @param key
    *   path in the state of the key: messages with the same key keep their order.
    * @param format
    *   `json` writes the value as JSON; `text` writes a string as it is.
    * @param headers
    *   fixed headers to add.
    * @param destinationType
    *   `queue` or `topic`; only ActiveMQ tells them apart.
    */
  final case class Params(
      connection: String,
      destination: String,
      payload: StatePath,
      key: Option[StatePath] = None,
      format: String = "json",
      headers: Map[String, String] = Map.empty,
      destinationType: String = "queue"
  )

  object Params {
    implicit val decoder: Decoder[Params] = Decoder.instance { c =>
      for {
        connection <- c.get[String]("connection")
        destination <- c.get[String]("destination")
        payload <- c.get[StatePath]("payload")
        key <- c.get[Option[StatePath]]("key")
        format <- c.getOrElse[String]("format")("json")
        headers <- c.getOrElse[Map[String, String]]("headers")(Map.empty)
        destinationType <- c.getOrElse[String]("destinationType")("queue")
      } yield Params(connection, destination, payload, key, format, headers, destinationType)
    }
  }

  /** What is sent. */
  final case class Message(key: Option[String], payload: Json, bytes: Array[Byte], headers: Map[String, String])

  def params(json: Json): Either[String, Params] = json.as[Params].leftMap(e => s"invalid configuration: ${e.message}")

  /** The message that the node sends in this state. */
  def build(params: Params, input: NodeInput): Either[String, Message] =
    for {
      payload <- params.payload.get(input.state).toRight(s"'${params.payload}' is not in the state")
      key <- params.key.traverse { path =>
        path.get(input.state).toRight(s"'$path' is not in the state").flatMap { v =>
          v.asString.orElse(if (v.isNumber || v.isBoolean) Some(v.noSpaces) else None).toRight(s"the key '$path' must be a string or a number")
        }
      }
      text <- params.format match {
        case "text" => payload.asString.toRight(s"'${params.payload}' is not a string, so it cannot be sent as text")
        case _      => Right(payload.noSpaces)
      }
    } yield Message(key, payload, text.getBytes(UTF_8), params.headers + (IdempotencyHeader -> s"${input.instanceId}:${input.nodeId}"))

  private def info(params: Params, message: Message, withPayload: Boolean): Map[String, Json] =
    Map(
      "connection" -> Json.fromString(params.connection),
      "destination" -> Json.fromString(params.destination),
      IdempotencyHeader -> Json.fromString(message.headers(IdempotencyHeader))
    ) ++ message.key.map(k => "key" -> Json.fromString(k)) ++
      (if (withPayload) Map("headers" -> Json.fromFields(message.headers.map { case (k, v) => k -> Json.fromString(v) }), "payload" -> message.payload) else Map.empty)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Messaging",
    label = "Publish",
    description = "Publishes a value of the state to a topic, queue or stream of a broker (Kafka, ActiveMQ, Redis Streams).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("connection", DataType.Str, description = Some("Name of the connection (Kafka, ActiveMQ or Redis)")),
        ConfigSchema.Field("destination", DataType.Str, description = Some("Topic, queue or stream")),
        ConfigSchema.Field("payload", DataType.Str, description = Some("Path in the state of the value to publish")),
        ConfigSchema.Field("key", DataType.Str, required = false, description = Some("Path in the state of the key that orders messages")),
        ConfigSchema.Field("format", DataType.Str, required = false, default = Some(Json.fromString("json")), oneOf = List(Json.fromString("json"), Json.fromString("text"))),
        ConfigSchema.Field("destinationType", DataType.Str, required = false, default = Some(Json.fromString("queue")), oneOf = List(Json.fromString("queue"), Json.fromString("topic")), description = Some("ActiveMQ only")),
        ConfigSchema.Field("headers", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Fixed headers, as text"))
      )
    ),
    sideEffects = SideEffects.Write,
    reads = json => params(json).toOption.toList.flatMap(p => StateRead(p.payload, DataType.AnyJson) :: p.key.map(StateRead(_, DataType.AnyJson)).toList),
    checkParams = json => params(json).left.toOption.toList
  )
}
