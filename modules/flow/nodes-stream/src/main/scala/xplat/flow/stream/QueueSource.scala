package xplat.flow.stream

import cats.effect.std.Queue
import cats.effect.{Async, Deferred}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.core.connectors.{Incoming, Sink}
import xplat.flow.http.TriggerDef
import xplat.flow.model._
import xplat.flow.nodes.Connections
import xplat.flow.queue.{ConsumerFactory, QueueKinds}

import java.nio.charset.StandardCharsets.UTF_8

/** The messages of a topic, queue or stream of a broker (Kafka, ActiveMQ, Redis Streams), as a stream that does not end. The broker
  * connection is a named one, as for `queue.publish`.
  *
  * A message is acknowledged to the broker only when the end of the segment has dealt with the element made from it (the sink
  * is done, or the element was dropped or failed), so what is lost if the server stops is nothing: the message comes again (at
  * least once). The consumer waits for that before it takes the next message, so one message is in flight at a time per source.
  * When the segment stops, the consumer is closed.
  */
final class QueueSource[F[_]](connections: Connections[F], factory: ConsumerFactory[F])(implicit F: Async[F]) extends StreamSource[F] {

  val descriptor: NodeDescriptor = QueueSource.descriptor

  def open(ctx: StreamContext[F]): Stream[F, Elem[F]] =
    QueueSource.params(ctx.params) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        Stream.eval(connections.resolve(p.connection)).flatMap {
          case None => Stream.raiseError[F](new IllegalArgumentException(s"the connection '${p.connection}' does not exist"))
          case Some(connection) =>
            Stream.eval(Queue.bounded[F, Option[Elem[F]]](1)).flatMap { queue =>
              val trigger = TriggerDef(
                id = ctx.node.id,
                flowId = "",
                kind = TriggerDef.Kind.Queue,
                queue = Some(p.source),
                connection = Some(p.connection),
                group = Some(p.group.getOrElse(s"flow-stream-${ctx.input.nodeId}")),
                destinationType = Some(p.destinationType)
              )
              val sink = Sink[F, Array[Byte]] { message =>
                for {
                  done <- Deferred[F, Unit]
                  at <- ctx.now
                  _ <- queue.offer(Some(Elem(QueueSource.value(p, message), at, done.complete(()).void)))
                  _ <- done.get
                } yield ()
              }
              Stream.resource(factory.consume(connection, trigger, sink)).flatMap { consumer =>
                Stream.fromQueueNoneTerminated(queue).concurrently(Stream.eval(consumer.stopped))
              }
            }
        }
    }
}

object QueueSource {
  val Kind: String = "stream.source.queue"

  /** @param source
    *   the topic, queue or stream.
    * @param format
    *   `json` reads the body as JSON (text if it is not); `text` reads it as text.
    * @param envelope
    *   the element is `{key, headers, source, position, body}` and not just the body.
    */
  final case class Params(connection: String, source: String, group: Option[String], destinationType: String, format: String, envelope: Boolean)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- c.get[String]("connection").left.map(_ => "missing field 'connection'")
      source <- c.get[String]("source").left.map(_ => "missing field 'source'")
      group <- c.get[Option[String]]("group").left.map(_.message)
      destinationType <- c.getOrElse[String]("destinationType")("queue").left.map(_.message)
      format <- c.getOrElse[String]("format")("json").left.map(_.message)
      envelope <- c.getOrElse[Boolean]("envelope")(false).left.map(_.message)
    } yield Params(connection, source, group.filter(_.nonEmpty), destinationType, format, envelope)
  }

  def value(p: Params, message: Incoming[Array[Byte]]): Json = {
    val text = new String(message.value, UTF_8)
    val body = if (p.format == "text") Json.fromString(text) else io.circe.parser.parse(text).getOrElse(Json.fromString(text))
    if (!p.envelope) body
    else
      Json.obj(
        "key" -> message.key.fold(Json.Null)(Json.fromString),
        "headers" -> Json.fromFields(message.headers.map { case (k, v) => k -> Json.fromString(v) }),
        "source" -> Json.fromString(message.source),
        "position" -> Json.fromString(message.position),
        "body" -> body
      )
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SourceCategory,
    label = "Queue",
    description = "Starts a stream, which does not end, from the messages of a topic, queue or stream of a broker.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("connection", DataType.Str, description = Some("Name of the connection (Kafka, ActiveMQ or Redis)")),
        ConfigSchema.Field("source", DataType.Str, description = Some("Topic, queue or stream to read")),
        ConfigSchema.Field("group", DataType.Str, required = false, description = Some("Consumer group; servers that share it share the messages")),
        ConfigSchema.Field("destinationType", DataType.Str, required = false, default = Some(Json.fromString("queue")), oneOf = List(Json.fromString("queue"), Json.fromString("topic")), description = Some("ActiveMQ only")),
        ConfigSchema.Field("format", DataType.Str, required = false, default = Some(Json.fromString("json")), oneOf = List(Json.fromString("json"), Json.fromString("text"))),
        ConfigSchema.Field("envelope", DataType.Bool, required = false, default = Some(Json.False), description = Some("Give the key, headers and position with the body"))
      )
    ),
    sideEffects = SideEffects.Read,
    ports = _ => StreamPorts.output(),
    checkParams = p => params(p).left.toOption.toList
  )

  /** The kinds of connection that this source can use. */
  val connectionKinds: Set[String] = Set(QueueKinds.Kafka, QueueKinds.ActiveMQ, QueueKinds.Redis)
}
