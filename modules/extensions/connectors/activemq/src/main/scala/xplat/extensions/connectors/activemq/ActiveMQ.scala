package xplat.extensions.connectors.activemq

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Mutex
import cats.implicits._
import jakarta.jms.{BytesMessage, ConnectionFactory, DeliveryMode, Message, Session, TextMessage}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import xplat.core.api.Codec
import xplat.core.connectors._

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

/** JMS consumers and producers that deliver to actors. They work with ActiveMQ Classic and ActiveMQ Artemis (or any
  * Jakarta JMS 3 broker): pass a `ConnectionFactory`, for example from [[ActiveMQ.artemis]] or [[ActiveMQ.classic]].
  *
  * Messages are processed one at a time and acknowledged after processing, so delivery is at least once. The message
  * key travels as the `JMSXGroupID` property: both brokers deliver the messages of one group in order to one consumer.
  * Headers travel as string properties, so their names must be valid Java identifiers.
  */
object ActiveMQ {

  sealed trait Destination {
    def name: String
  }

  object Destination {

    /** Each message goes to one consumer. */
    final case class Queue(name: String) extends Destination

    /** Each message goes to every subscription. With `durableSubscription`, the subscription keeps the messages
      * published while its consumer is down (it needs a `clientId` in the consumer config); without it, they are lost.
      */
    final case class Topic(name: String, durableSubscription: Option[String] = None) extends Destination
  }

  /** @param clientId
    *   JMS client id of the connection, required by durable topic subscriptions.
    * @param receiveTimeout
    *   how long each receive waits; it bounds how long stopping the consumer takes.
    */
  final case class ConsumerConfig(
      retry: RetryPolicy = RetryPolicy(),
      clientId: Option[String] = None,
      receiveTimeout: FiniteDuration = 1.second
  )

  /** Factory for ActiveMQ Artemis. Needs `org.apache.activemq:artemis-jakarta-client` on the classpath. */
  def artemis(url: String, user: Option[String] = None, password: Option[String] = None): ConnectionFactory =
    new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(url, user.orNull, password.orNull)

  /** Factory for ActiveMQ Classic 6 or later. Needs `org.apache.activemq:activemq-client` on the classpath. */
  def classic(url: String, user: Option[String] = None, password: Option[String] = None): ConnectionFactory =
    new org.apache.activemq.ActiveMQConnectionFactory(user.orNull, password.orNull, url)

  private val GroupProperty = "JMSXGroupID"

  private def session[F[_]](factory: ConnectionFactory, clientId: Option[String], ackMode: Int)(implicit A: Async[F]) =
    for {
      connection <- Resource.make(A.blocking {
                      val c = factory.createConnection()
                      clientId.foreach(c.setClientID)
                      c
                    })(c => A.blocking(c.close()))
      session <- Resource.make(A.blocking(connection.createSession(false, ackMode)))(s => A.blocking(s.close()))
    } yield (connection, session)

  private def jmsDestination(session: Session, destination: Destination): jakarta.jms.Destination = destination match {
    case Destination.Queue(name)    => session.createQueue(name)
    case Destination.Topic(name, _) => session.createTopic(name)
  }

  def consumer[F[_], T](factory: ConnectionFactory, destination: Destination, config: ConsumerConfig = ConsumerConfig())(sink: Sink[F, T])(implicit
      codec: Codec[T, Array[Byte]],
      A: Async[F]
  ): Resource[F, RunningConsumer[F]] =
    for {
      log           <- Resource.eval(Slf4jLogger.fromName[F]("xplat.connectors.activemq"))
      opened        <- session[F](factory, config.clientId, Session.CLIENT_ACKNOWLEDGE)
      (conn, sess)   = opened
      jmsConsumer <- Resource.make(A.blocking(destination match {
                       case Destination.Topic(name, Some(subscription)) => sess.createDurableSubscriber(sess.createTopic(name), subscription)
                       case other                                       => sess.createConsumer(jmsDestination(sess, other))
                     }))(c => A.blocking(c.close()))
      _ <- Resource.eval(A.blocking(conn.start()))
      running <- {
        def body(message: Message): Array[Byte] = message match {
          case text: TextMessage => text.getText.getBytes(UTF_8)
          case bytes: BytesMessage =>
            val out = new Array[Byte](bytes.getBodyLength.toInt)
            bytes.readBytes(out)
            out
          case other => other.getBody(classOf[Array[Byte]])
        }

        def process(message: Message): F[Unit] = {
          val position = String.valueOf(message.getJMSMessageID)
          val decoded = A.blocking {
            val headers = message.getPropertyNames.asScala.collect {
              case name: String if !name.startsWith("JMS") => name -> String.valueOf(message.getObjectProperty(name))
            }.toMap
            val key = Option(message.getStringProperty(GroupProperty))
            (body(message), key, headers)
          }
          decoded.flatMap { case (bytes, key, headers) =>
            val incoming = Consuming.decode(codec, destination.name, position, bytes).map(Incoming(destination.name, key, _, headers, position))
            Consuming.deliver(sink, config.retry, log)(destination.name, position, incoming)
          } >> A.blocking(message.acknowledge())
        }

        lazy val loop: F[Unit] =
          A.blocking(Option(jmsConsumer.receive(config.receiveTimeout.toMillis))).flatMap {
            case Some(message) => process(message) >> loop
            case None          => loop
          }

        Consuming.background(loop)
      }
    } yield running

  /** Publishes persistent messages to `destination`; `publish` completes when the broker has stored the message. */
  def publisher[F[_], T](factory: ConnectionFactory, destination: Destination)(implicit codec: Codec[T, Array[Byte]], A: Async[F]): Resource[F, Publisher[F, T]] = {
    val target = destination
    for {
      opened <- session[F](factory, None, Session.AUTO_ACKNOWLEDGE)
      sess    = opened._2
      producer <- Resource.make(A.blocking {
                    val p = sess.createProducer(jmsDestination(sess, target))
                    p.setDeliveryMode(DeliveryMode.PERSISTENT)
                    p
                  })(p => A.blocking(p.close()))
      // A JMS session must not be used by two threads at once.
      lock <- Resource.eval(Mutex[F])
    } yield new Publisher[F, T] {
      val destination: String = target.name

      def publish(message: Outgoing[T]): F[Unit] =
        A.fromEither(codec.encode(message.value).leftMap(e => new IllegalArgumentException(s"cannot encode for ${target.name}: ${e.message}"))).flatMap {
          bytes =>
            lock.lock.surround(A.blocking {
              val jms = sess.createBytesMessage()
              jms.writeBytes(bytes)
              message.key.foreach(jms.setStringProperty(GroupProperty, _))
              message.headers.foreach { case (k, v) => jms.setStringProperty(k, v) }
              producer.send(jms)
            })
        }
    }
  }
}
