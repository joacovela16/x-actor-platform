package xplat.extensions.connectors.kafka

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.implicits._
import fs2.kafka._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import xplat.core.api.Codec
import xplat.core.connectors._

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Kafka consumers and producers that deliver to actors.
  *
  * Messages are processed one at a time per partition, so messages with the same key arrive in order; partitions are
  * processed in parallel. An offset is committed only after its message was processed, so delivery is at least once.
  */
object Kafka {

  /** @param bootstrapServers
    *   `host:port` of one or more brokers, comma separated.
    * @param properties
    *   any other Kafka client property (security, timeouts, ...), applied to consumers and producers.
    */
  final case class Config(bootstrapServers: String, properties: Map[String, String] = Map.empty)

  /** @param groupId
    *   consumers with the same group share the partitions and the committed offsets.
    * @param fromBeginning
    *   where a group without committed offsets starts: the oldest message (`true`) or only new ones.
    * @param commitEvery
    *   offsets are committed after this many processed messages per partition, or after `commitWithin`.
    */
  final case class ConsumerConfig(
      groupId: String,
      topics: List[String],
      fromBeginning: Boolean = true,
      retry: RetryPolicy = RetryPolicy(),
      commitEvery: Int = 100,
      commitWithin: FiniteDuration = 1.second
  ) {
    require(topics.nonEmpty, "topics must not be empty")
  }

  def consumer[F[_], T](config: Config, consumer: ConsumerConfig)(sink: Sink[F, T])(implicit
      codec: Codec[T, Array[Byte]],
      A: Async[F]
  ): Resource[F, RunningConsumer[F]] = {
    val settings = ConsumerSettings(Deserializer[F, String].option, Deserializer[F, Array[Byte]])
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(consumer.groupId)
      .withAutoOffsetReset(if (consumer.fromBeginning) AutoOffsetReset.Earliest else AutoOffsetReset.Latest)
      .withEnableAutoCommit(false)
      .withProperties(config.properties)

    Resource.eval(Slf4jLogger.fromName[F]("xplat.connectors.kafka")).flatMap { log =>
      def process(record: ConsumerRecord[Option[String], Array[Byte]]): F[Unit] = {
        val position = s"${record.partition}@${record.offset}"
        val message = Consuming.decode(codec, record.topic, position, record.value).map { value =>
          val headers = record.headers.toChain.toList.map(h => h.key -> new String(h.value, UTF_8)).toMap
          Incoming(record.topic, record.key, value, headers, position)
        }
        Consuming.deliver(sink, consumer.retry, log)(record.topic, position, message)
      }

      val loop = KafkaConsumer
        .stream(settings)
        .evalTap(_.subscribe(NonEmptyList.fromListUnsafe(consumer.topics)))
        .flatMap(_.partitionedStream)
        .map(_.evalMap(c => process(c.record).as(c.offset)).through(commitBatchWithin[F](consumer.commitEvery, consumer.commitWithin)))
        .parJoinUnbounded
        .compile
        .drain

      Consuming.background(loop)
    }
  }

  /** Publishes to `topic`; `publish` completes when all in-sync replicas stored the message (`acks=all`). */
  def publisher[F[_], T](config: Config, topic: String)(implicit codec: Codec[T, Array[Byte]], A: Async[F]): Resource[F, Publisher[F, T]] = {
    val settings = ProducerSettings(Serializer[F, String].option, Serializer[F, Array[Byte]])
      .withBootstrapServers(config.bootstrapServers)
      .withAcks(Acks.All)
      .withEnableIdempotence(true)
      .withProperties(config.properties)

    KafkaProducer.resource(settings).map { producer =>
      new Publisher[F, T] {
        val destination: String = topic

        def publish(message: Outgoing[T]): F[Unit] =
          A.fromEither(codec.encode(message.value).leftMap(e => new IllegalArgumentException(s"cannot encode for $topic: ${e.message}"))).flatMap { bytes =>
            val headers = Headers.fromSeq(message.headers.toSeq.map { case (k, v) => Header(k, v.getBytes(UTF_8)) })
            producer.produceOne(ProducerRecord(topic, message.key, bytes).withHeaders(headers)).flatten.void
          }
      }
    }
  }
}
