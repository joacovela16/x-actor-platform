package xplat.flow.queue

import cats.effect.{Async, Resource}
import xplat.core.connectors.{Outgoing, Publisher}
import xplat.extensions.connectors.activemq.ActiveMQ
import xplat.extensions.connectors.kafka.{Kafka => KafkaConnector}
import xplat.extensions.connectors.redis.RedisStreams
import xplat.flow.nodes.Connections

/** Opens the publisher of a connection. The real one uses the connectors (Kafka, ActiveMQ, Redis); tests give another. */
trait PublisherFactory[F[_]] {

  /** @param destinationType
    *   `queue` or `topic`; only ActiveMQ tells them apart.
    */
  def open(connection: Connections.Resolved, destination: String, destinationType: String): Resource[F, Publisher[F, Array[Byte]]]
}

object PublisherFactory {

  def connectors[F[_]](implicit A: Async[F]): PublisherFactory[F] = new PublisherFactory[F] {
    def open(c: Connections.Resolved, destination: String, destinationType: String): Resource[F, Publisher[F, Array[Byte]]] =
      c.kind match {
        case QueueKinds.Kafka =>
          c.text("bootstrapServers") match {
            case None          => failed(s"connection '${c.name}' has no bootstrapServers")
            case Some(servers) => KafkaConnector.publisher[F, Array[Byte]](KafkaConnector.Config(servers, QueueKinds.kafkaProperties(c)), destination)
          }
        case QueueKinds.ActiveMQ =>
          c.text("url") match {
            case None => failed(s"connection '${c.name}' has no url")
            case Some(url) =>
              val user = c.text("username")
              val password = c.secrets.get("password").filter(_.nonEmpty)
              val factory = if (c.text("flavor").contains("classic")) ActiveMQ.classic(url, user, password) else ActiveMQ.artemis(url, user, password)
              val target = if (destinationType == "topic") ActiveMQ.Destination.Topic(destination) else ActiveMQ.Destination.Queue(destination)
              ActiveMQ.publisher[F, Array[Byte]](factory, target).map(jmsSafe)
          }
        case QueueKinds.Redis =>
          QueueKinds.redisUri(c) match {
            case Left(problem) => failed(problem)
            case Right(uri)    => RedisStreams.publisher[F, Array[Byte]](uri, destination)
          }
        case other => failed(s"connection '${c.name}' is of kind '$other', which is not a message broker")
      }

    private def failed(problem: String): Resource[F, Publisher[F, Array[Byte]]] =
      Resource.eval(A.raiseError[Publisher[F, Array[Byte]]](new IllegalArgumentException(problem)))
  }

  /** JMS property names must be Java identifiers, so `x-idempotency-key` is sent as `x_idempotency_key`. */
  private def jmsSafe[F[_]](p: Publisher[F, Array[Byte]]): Publisher[F, Array[Byte]] = new Publisher[F, Array[Byte]] {
    val destination: String = p.destination
    def publish(message: Outgoing[Array[Byte]]): F[Unit] =
      p.publish(message.copy(headers = message.headers.map { case (k, v) => k.replaceAll("[^A-Za-z0-9_$]", "_") -> v }))
  }
}
