package xplat.extensions.connectors.redis

import cats.effect.kernel.{Async, Resource}
import cats.implicits._
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.data.RedisCodec
import dev.profunktor.redis4cats.effect.Log
import dev.profunktor.redis4cats.effects._
import io.lettuce.core.codec.{ByteArrayCodec, StringCodec, RedisCodec => JRedisCodec}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import xplat.core.api.Codec
import xplat.core.connectors._

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Redis Streams consumers and producers that deliver to actors.
  *
  * Consumers read through a consumer group (`XREADGROUP`) and acknowledge (`XACK`) each message after processing it, so
  * delivery is at least once. Messages are processed one at a time, in stream order.
  *
  * A message read but not acknowledged stays pending for the consumer that read it. A consumer that starts again with
  * the same name first processes its own pending messages; with `claimIdleAfter` it also takes over the messages that
  * other consumers of the group left pending for that long (for example because they crashed for good).
  */
object RedisStreams {

  /** @param consumer
    *   name of this consumer in the group. Keep it stable across restarts, e.g. the host name.
    * @param fromBeginning
    *   where a new group starts: the oldest message (`true`) or only new ones.
    * @param batchSize
    *   messages read per call.
    * @param block
    *   how long each read waits for new messages; it bounds how long stopping the consumer takes.
    * @param claimIdleAfter
    *   take over messages that other consumers of the group left pending for this long.
    */
  final case class ConsumerConfig(
      stream: String,
      group: String,
      consumer: String,
      retry: RetryPolicy = RetryPolicy(),
      fromBeginning: Boolean = true,
      batchSize: Long = 10,
      block: FiniteDuration = 1.second,
      claimIdleAfter: Option[FiniteDuration] = None
  )

  // Fields of a stream entry.
  private val ValueField = "value"
  private val KeyField = "key"
  private val HeaderPrefix = "header:"

  private val codec: RedisCodec[String, Array[Byte]] = RedisCodec(JRedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE))

  private def commands[F[_]: Async](uri: String) = {
    implicit val log: Log[F] = Log.NoOp.instance[F]
    Redis[F].simple(uri, codec)
  }

  def consumer[F[_], T](uri: String, config: ConsumerConfig)(sink: Sink[F, T])(implicit
      valueCodec: Codec[T, Array[Byte]],
      A: Async[F]
  ): Resource[F, RunningConsumer[F]] =
    for {
      log   <- Resource.eval(Slf4jLogger.fromName[F]("xplat.connectors.redis"))
      redis <- commands[F](uri)
      running <- {
        val stream = config.stream
        val me = StreamConsumer(config.group, config.consumer)

        val createGroup: F[Unit] =
          redis.xGroupCreate(stream, config.group, if (config.fromBeginning) "0" else "$", XGroupCreateArgs(mkStream = true)).recover {
            case e if String.valueOf(e.getMessage).contains("BUSYGROUP") => () // it already exists
          }

        def process(message: StreamMessage[String, Array[Byte]]): F[Unit] = {
          val position = message.id.value
          val handled = message.body.get(ValueField) match {
            case None => A.unit // deleted from the stream after it was read: nothing to process
            case Some(bytes) =>
              val key = message.body.get(KeyField).map(new String(_, UTF_8))
              val headers = message.body.collect { case (k, v) if k.startsWith(HeaderPrefix) => k.drop(HeaderPrefix.length) -> new String(v, UTF_8) }
              val incoming = Consuming.decode(valueCodec, stream, position, bytes).map(Incoming(stream, key, _, headers, position))
              Consuming.deliver(sink, config.retry, log)(stream, position, incoming)
          }
          handled >> redis.xAck(stream, config.group, position).void
        }

        def read(offset: String, block: Option[FiniteDuration]): F[List[StreamMessage[String, Array[Byte]]]] =
          redis.xReadGroup(me, XReadOffsets.custom(offset, stream), XReadGroupArgs(count = Some(config.batchSize), block = block))

        // Messages this consumer read before stopping, oldest first.
        lazy val ownPending: F[Unit] =
          read("0", None).flatMap(messages => if (messages.isEmpty) A.unit else messages.traverse_(process) >> ownPending)

        val claimIdle: F[Unit] = config.claimIdleAfter.traverse_ { idle =>
          redis.xAutoClaim(stream, XAutoClaimArgs(me, idle, count = Some(config.batchSize))).flatMap(_.messages.traverse_(process))
        }

        lazy val loop: F[Unit] =
          claimIdle >> read(">", Some(config.block)).flatMap(_.traverse_(process)) >> loop

        Consuming.background(createGroup >> ownPending >> loop)
      }
    } yield running

  /** Appends to `stream` (`XADD`). With `maxLength`, old entries are trimmed so the stream keeps about that many. */
  def publisher[F[_], T](uri: String, stream: String, maxLength: Option[Long] = None)(implicit
      valueCodec: Codec[T, Array[Byte]],
      A: Async[F]
  ): Resource[F, Publisher[F, T]] =
    commands[F](uri).map { redis =>
      val trim = maxLength.map(n => XTrimArgs(XTrimArgs.Strategy.MAXLEN(n), XTrimArgs.Precision.Approximate()))
      new Publisher[F, T] {
        val destination: String = stream

        def publish(message: Outgoing[T]): F[Unit] =
          A.fromEither(valueCodec.encode(message.value).leftMap(e => new IllegalArgumentException(s"cannot encode for $stream: ${e.message}"))).flatMap {
            bytes =>
              val body = Map(ValueField -> bytes) ++
                message.key.map(k => KeyField -> k.getBytes(UTF_8)) ++
                message.headers.map { case (k, v) => s"$HeaderPrefix$k" -> v.getBytes(UTF_8) }
              redis.xAdd(stream, body, XAddArgs(xTrimArgs = trim)).void
          }
      }
    }
}
