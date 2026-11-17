package xplat.flow.queue

import cats.effect.{Async, Resource}
import xplat.core.connectors.{RunningConsumer, Sink}
import xplat.extensions.connectors.activemq.ActiveMQ
import xplat.extensions.connectors.kafka.{Kafka => KafkaConnector}
import xplat.extensions.connectors.redis.RedisStreams
import xplat.flow.http.TriggerDef
import xplat.flow.nodes.Connections

/** Starts the consumer of a `queue` trigger. The real one uses the connectors (Kafka, ActiveMQ, Redis), which retry and
  * acknowledge as their `RetryPolicy` says; tests give another.
  */
trait ConsumerFactory[F[_]] {
  def consume(connection: Connections.Resolved, trigger: TriggerDef, sink: Sink[F, Array[Byte]]): Resource[F, RunningConsumer[F]]
}

object ConsumerFactory {

  /** The consumer group of a trigger. */
  def groupOf(trigger: TriggerDef): String = trigger.consumerGroup

  def connectors[F[_]](implicit A: Async[F]): ConsumerFactory[F] = new ConsumerFactory[F] {
    def consume(c: Connections.Resolved, trigger: TriggerDef, sink: Sink[F, Array[Byte]]): Resource[F, RunningConsumer[F]] = {
      val source = trigger.queue.getOrElse("")
      val group = groupOf(trigger)
      c.kind match {
        case QueueKinds.Kafka =>
          c.text("bootstrapServers") match {
            case None => failed(s"connection '${c.name}' has no bootstrapServers")
            case Some(servers) =>
              KafkaConnector.consumer[F, Array[Byte]](KafkaConnector.Config(servers, QueueKinds.kafkaProperties(c)), KafkaConnector.ConsumerConfig(group, List(source)))(sink)
          }
        case QueueKinds.ActiveMQ =>
          c.text("url") match {
            case None => failed(s"connection '${c.name}' has no url")
            case Some(url) =>
              val user = c.text("username")
              val password = c.secrets.get("password").filter(_.nonEmpty)
              val factory = if (c.text("flavor").contains("classic")) ActiveMQ.classic(url, user, password) else ActiveMQ.artemis(url, user, password)
              // A topic is read with a durable subscription named after the group, so no message is lost while the server is down.
              if (trigger.destinationType.contains("topic"))
                ActiveMQ.consumer[F, Array[Byte]](factory, ActiveMQ.Destination.Topic(source, Some(group)), ActiveMQ.ConsumerConfig(clientId = Some(group)))(sink)
              else ActiveMQ.consumer[F, Array[Byte]](factory, ActiveMQ.Destination.Queue(source))(sink)
          }
        case QueueKinds.Redis =>
          QueueKinds.redisUri(c) match {
            case Left(problem) => failed(problem)
            case Right(uri)    => RedisStreams.consumer[F, Array[Byte]](uri, RedisStreams.ConsumerConfig(stream = source, group = group, consumer = hostName))(sink)
          }
        case other => failed(s"connection '${c.name}' is of kind '$other', which is not a message broker")
      }
    }

    private def failed(problem: String): Resource[F, RunningConsumer[F]] =
      Resource.eval(A.raiseError[RunningConsumer[F]](new IllegalArgumentException(problem)))
  }

  private lazy val hostName: String = scala.util.Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("flow")
}
