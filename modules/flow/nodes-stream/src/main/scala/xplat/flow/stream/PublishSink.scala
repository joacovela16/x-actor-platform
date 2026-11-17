package xplat.flow.stream

import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.core.connectors.Outgoing
import xplat.flow.model._
import xplat.flow.nodes.{Connections, Traces}
import xplat.flow.queue.{PublishNode, PublisherPool}

import java.nio.charset.StandardCharsets.UTF_8

/** Publishes each element to a topic, queue or stream of a broker, as `queue.publish` does with a value of the state. The message
  * is the element as JSON (or as text, with `format: text`) and carries `x-idempotency-key` = `<instance>:<node>:<number>`, which a
  * consumer can use to drop the duplicate that a retry makes. An element that cannot be published is a dead letter. It writes to the
  * first output it declares how many were published.
  */
final class PublishSink[F[_]](connections: Connections[F], pool: PublisherPool[F], traces: Option[Traces[F]] = None)(implicit F: Async[F]) extends StreamSink[F] {

  val descriptor: NodeDescriptor = PublishSink.descriptor

  def drain(ctx: StreamContext[F], upstream: Stream[F, Elem[F]]): F[Map[String, Json]] =
    PublishSink.params(ctx.params) match {
      case Left(problem) => F.raiseError(new IllegalArgumentException(problem))
      case Right(p) =>
        // The publisher is lent for the whole stream: the pool does not close it while elements are being sent.
        connections.resolve(p.connection).flatMap(F.fromOption(_, new IllegalArgumentException(s"the connection '${p.connection}' does not exist"))).flatMap { connection =>
        pool.use(connection, p.destination, p.destinationType) { publisher =>
        for {
          // Every message carries the `traceparent` of the segment, unless the headers set one.
          traceparent <-
            if (p.headers.keys.exists(_.equalsIgnoreCase(Traces.Header))) F.pure(Option.empty[String])
            else traces.fold(F.pure(Option.empty[String]))(_.traceparent(ctx.input.instanceId, ctx.input.nodeId))
          sent <- Ref.of[F, Long](0L)
          _ <- upstream.evalMap { e =>
            PublishSink.message(p, e.value) match {
              case Left(error) => ctx.fail(e, error)
              case Right((bytes, key)) =>
                sent.modify(n => (n + 1, n + 1)).flatMap { n =>
                  val headers = p.headers ++ traceparent.map(Traces.Header -> _) + (PublishNode.IdempotencyHeader -> s"${ctx.input.instanceId}:${ctx.input.nodeId}:$n")
                  publisher.publish(Outgoing(bytes, key, headers)).attempt.flatMap {
                    case Right(_)    => e.ack
                    case Left(error) => sent.update(_ - 1) >> ctx.fail(e, s"cannot publish to ${p.destination}: ${Option(error.getMessage).getOrElse(error.toString)}")
                  }
                }
            }
          }.compile.drain
          n <- sent.get
        } yield ctx.result(Json.fromLong(n))
        }
        }
    }
}

object PublishSink {
  val Kind: String = "stream.sink.publish"

  final case class Params(connection: String, destination: String, destinationType: String, key: Option[StatePath], format: String, headers: Map[String, String])

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- c.get[String]("connection").left.map(_ => "missing field 'connection'")
      destination <- c.get[String]("destination").left.map(_ => "missing field 'destination'")
      destinationType <- c.getOrElse[String]("destinationType")("queue").left.map(_.message)
      key <- c.get[Option[String]]("key").left.map(_.message).flatMap(_.filter(_.nonEmpty).traverse(StatePath.parse))
      format <- c.getOrElse[String]("format")("json").left.map(_.message)
      headers <- c.getOrElse[Map[String, String]]("headers")(Map.empty).left.map(_.message)
    } yield Params(connection, destination, destinationType, key, format, headers)
  }

  def message(p: Params, element: Json): Either[String, (Array[Byte], Option[String])] =
    for {
      key <- p.key.traverse { path =>
        path.get(element).toRight(s"'$path' is not in the element").flatMap(v => v.asString.orElse(if (v.isNumber || v.isBoolean) Some(v.noSpaces) else None).toRight(s"the key '$path' must be a string or a number"))
      }
      text <- if (p.format == "text") element.asString.toRight("the element is not a string, so it cannot be sent as text") else Right(element.noSpaces)
    } yield (text.getBytes(UTF_8), key)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SinkCategory,
    label = "Publish each",
    description = "Ends a stream by publishing each element to a topic, queue or stream of a broker.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("connection", DataType.Str, description = Some("Name of the connection (Kafka, ActiveMQ or Redis)")),
        ConfigSchema.Field("destination", DataType.Str, description = Some("Topic, queue or stream")),
        ConfigSchema.Field("destinationType", DataType.Str, required = false, default = Some(Json.fromString("queue")), oneOf = List(Json.fromString("queue"), Json.fromString("topic")), description = Some("ActiveMQ only")),
        ConfigSchema.Field("key", DataType.Str, required = false, description = Some("Path in the element of the key that orders messages")),
        ConfigSchema.Field("format", DataType.Str, required = false, default = Some(Json.fromString("json")), oneOf = List(Json.fromString("json"), Json.fromString("text"))),
        ConfigSchema.Field("headers", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Fixed headers, as text"))
      )
    ),
    sideEffects = SideEffects.Write,
    inputs = _ => StreamPorts.input,
    defaultOutputs = _ => List(OutputDecl("result", StatePath(List("published")), DataType.Num)),
    checkParams = p => params(p).left.toOption.toList
  )
}
