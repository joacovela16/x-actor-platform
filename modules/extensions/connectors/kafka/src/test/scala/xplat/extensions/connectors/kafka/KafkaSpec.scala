package xplat.extensions.connectors.kafka

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import xplat.core.connectors.Codecs.utf8
import xplat.core.connectors._
import xplat.extensions.persistence.testkit.Containers

/** One Kafka broker (KRaft, single node) for the whole run. It advertises the host port, so it is fixed beforehand. */
object KafkaSpec {
  private lazy val started: Option[(Containers.Container, IO[Unit])] =
    if (!Containers.available) None
    else {
      val port = Containers.freePort()
      val env = Map(
        "KAFKA_NODE_ID" -> "1",
        "KAFKA_PROCESS_ROLES" -> "broker,controller",
        "KAFKA_LISTENERS" -> s"PLAINTEXT://:$port,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS" -> s"PLAINTEXT://127.0.0.1:$port",
        "KAFKA_CONTROLLER_LISTENER_NAMES" -> "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" -> "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS" -> "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" -> "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR" -> "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS" -> "0"
      )
      Some(
        Containers
          .container("docker.io/apache/kafka:3.9.1", port, env, hostPort = Some(port)) { c =>
            Kafka.publisher[IO, String](Kafka.Config(s"${c.host}:${c.port}"), "ready").use(_.publish("ping"))
          }
          .allocated
          .unsafeRunSync()
      )
    }

  def available: Boolean = started.isDefined
  def config: Kafka.Config = Kafka.Config(s"${started.get._1.host}:${started.get._1.port}")

  sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
}

class KafkaConnectorSpec extends ConnectorContract {
  override def available: Boolean = KafkaSpec.available

  def publisher(destination: String): Resource[IO, Publisher[IO, String]] = Kafka.publisher[IO, String](KafkaSpec.config, destination)

  def consumer(destination: String, group: String, retry: RetryPolicy)(sink: Sink[IO, String]): Resource[IO, RunningConsumer[IO]] =
    Kafka.consumer[IO, String](KafkaSpec.config, Kafka.ConsumerConfig(group, List(destination), retry = retry))(sink)
}
