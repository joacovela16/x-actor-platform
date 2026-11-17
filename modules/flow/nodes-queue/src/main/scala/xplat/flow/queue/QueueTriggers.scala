package xplat.flow.queue

import cats.effect.{Async, Resource}
import cats.syntax.all._
import io.circe.Json
import xplat.core.connectors.{Incoming, Sink}
import xplat.flow.engine.FlowEngine
import xplat.flow.http.{TriggerDef, TriggerRegistry, TriggerSources}
import xplat.flow.nodes.{Connections, TriggerSource}

import scala.concurrent.duration._

/** The source of the `queue` triggers on the brokers' connections: a consumer of the trigger's topic, queue or stream that starts
  * an instance of its flow for every message, which it acknowledges once the instance has started (see [[TriggerSources]] for how
  * the server runs it).
  *
  *   - The message becomes a request (`source`, `key`, `position`, `headers`, `body`: the JSON of the body, or the text if it is
  *     not JSON) and the `inputs` of the trigger map it to the inputs of the flow, as they do for HTTP.
  *   - Its key is its `x-idempotency-key`, `idempotency-key` or `message-id` header, or else its place in the broker.
  *   - A failure to start the instance is retried as the consumer's `RetryPolicy` says.
  */
object QueueTriggers {

  final case class Settings(pollEvery: FiniteDuration = 2.seconds, restartAfter: FiniteDuration = 5.seconds)

  /** The id of the instance for a message. */
  def instanceId(trigger: TriggerDef, message: Incoming[Array[Byte]]): String = TriggerSources.instanceId(trigger, key(message))

  /** What identifies a message in its broker. */
  def key(message: Incoming[Array[Byte]]): String = TriggerSources.messageKey(message.source, message.position, message.headers)

  /** The request that the message makes, to map into inputs. */
  def request(message: Incoming[Array[Byte]]): Json =
    TriggerSources.messageRequest(message.source, message.key, message.position, message.headers, message.value)

  /** What a source gives the consumer: each message starts an instance. */
  def sink[F[_]: Async](start: TriggerSource.Start[F]): Sink[F, Array[Byte]] =
    Sink[F, Array[Byte]](message => start(request(message), key(message), Some(message.position)).void)

  /** `queue` triggers on the connections whose kind `accepts`, read with `factory`. */
  def source[F[_]](factory: ConsumerFactory[F], accept: String => Boolean)(implicit A: Async[F]): TriggerSource[F] = new TriggerSource[F] {
    val kind: String = TriggerDef.Kind.Queue.name
    def accepts(connectionKind: String): Boolean = accept(connectionKind)
    def run(trigger: Json, connection: Connections.Resolved, start: TriggerSource.Start[F]): F[Unit] =
      A.fromEither(trigger.as[TriggerDef]).flatMap(t => factory.consume(connection, t, sink(start)).use(_.stopped))
  }

  /** The `queue` triggers of `triggers`, all read with `factory`, run as the server runs them. */
  def resource[F[_]](
      engine: FlowEngine[F],
      triggers: TriggerRegistry[F],
      connections: Connections[F],
      factory: ConsumerFactory[F],
      settings: Settings = Settings()
  )(implicit A: Async[F]): Resource[F, Unit] =
    TriggerSources.resource[F](engine, triggers, connections, List(source(factory, _ => true)), TriggerSources.Settings(settings.pollEvery, settings.restartAfter)).void
}
