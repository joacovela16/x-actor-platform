package xplat.extensions.connectors.activemq

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import jakarta.jms.ConnectionFactory
import xplat.core.connectors.Codecs.utf8
import xplat.core.connectors._
import xplat.extensions.persistence.testkit.Containers

/** One broker container per flavour, started on first use and shared by the run. */
object ActiveMQSpec {

  final class Broker(image: String, env: Map[String, String], factoryFor: String => ConnectionFactory) {
    private lazy val started: Option[(Containers.Container, IO[Unit])] =
      if (!Containers.available) None
      else
        Some(
          Containers
            .container(image, 61616, env) { c =>
              IO.blocking(factoryFor(url(c)).createConnection().close())
            }
            .allocated
            .unsafeRunSync()
        )

    private def url(c: Containers.Container) = s"tcp://${c.host}:${c.port}"

    def available: Boolean = started.isDefined
    def factory: ConnectionFactory = factoryFor(url(started.get._1))

    sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
  }

  val artemis = new Broker(
    "docker.io/apache/activemq-artemis:2.40.0-alpine",
    Map("ARTEMIS_USER" -> "xplat", "ARTEMIS_PASSWORD" -> "xplat"),
    url => ActiveMQ.artemis(url, Some("xplat"), Some("xplat"))
  )

  val classic = new Broker("docker.io/apache/activemq-classic:6.1.6", Map.empty, url => ActiveMQ.classic(url))
}

abstract class ActiveMQContract(broker: ActiveMQSpec.Broker) extends ConnectorContract {
  override def available: Boolean = broker.available

  def publisher(destination: String): Resource[IO, Publisher[IO, String]] =
    ActiveMQ.publisher[IO, String](broker.factory, ActiveMQ.Destination.Queue(destination))

  // Queues have no groups: every consumer of the queue shares its messages.
  def consumer(destination: String, group: String, retry: RetryPolicy)(sink: Sink[IO, String]): Resource[IO, RunningConsumer[IO]] =
    ActiveMQ.consumer[IO, String](broker.factory, ActiveMQ.Destination.Queue(destination), ActiveMQ.ConsumerConfig(retry))(sink)
}

class ArtemisConnectorSpec extends ActiveMQContract(ActiveMQSpec.artemis)

class ClassicConnectorSpec extends ActiveMQContract(ActiveMQSpec.classic)
