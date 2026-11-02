package xplat.extensions.connectors.redis

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource, Ref}
import scala.concurrent.duration._
import xplat.core.connectors._
import xplat.extensions.persistence.testkit.Containers

/** One Redis container for the whole run. */
object RedisSpec {
  private lazy val started: Option[(Containers.Container, IO[Unit])] =
    if (!Containers.available) None
    else
      Some(
        Containers
          .container("docker.io/library/redis:7-alpine", 6379) { c =>
            RedisStreams.publisher[IO, String](uri(c), "ready").use(_.publish("ping"))
          }
          .allocated
          .unsafeRunSync()
      )

  private def uri(c: Containers.Container) = s"redis://${c.host}:${c.port}"

  def available: Boolean = started.isDefined
  def uri: String = uri(started.get._1)

  sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
}

class RedisConnectorSpec extends ConnectorContract {
  override def available: Boolean = RedisSpec.available

  def publisher(destination: String): Resource[IO, Publisher[IO, String]] = RedisStreams.publisher[IO, String](RedisSpec.uri, destination)

  // A consumer that restarts keeps its name, so it resumes its pending messages.
  def consumer(destination: String, group: String, retry: RetryPolicy)(sink: Sink[IO, String]): Resource[IO, RunningConsumer[IO]] =
    RedisStreams.consumer[IO, String](RedisSpec.uri, RedisStreams.ConsumerConfig(destination, group, s"$group-consumer", retry))(sink)

  "redis streams" - {
    "a consumer with claimIdleAfter takes over what another consumer left pending" in {
      assume(available, "redis is not available")
      val stream = s"claims-${java.util.UUID.randomUUID().toString.take(8)}"
      val fast = RetryPolicy(maxRetries = 0, initialBackoff = 10.millis)
      def config(name: String, claim: Option[FiniteDuration]) = RedisStreams.ConsumerConfig(stream, "group", name, fast, claimIdleAfter = claim)
      (for {
        received <- Ref[IO].of(Vector.empty[String])
        _ <- publisher(stream).use(_.publish("orphan"))
        // "crashed" reads it and stops without acknowledging; it never comes back
        _ <- RedisStreams.consumer[IO, String](RedisSpec.uri, config("crashed", None))(Sink[IO, String](_ => IO.raiseError(new RuntimeException("down")))).use(_.stopped.attempt)
        got <- RedisStreams.consumer[IO, String](RedisSpec.uri, config("rescuer", Some(100.millis)))(Sink[IO, String](m => received.update(_ :+ m.value))).use { _ =>
                 def wait: IO[Vector[String]] = received.get.flatMap(r => if (r.nonEmpty) IO.pure(r) else IO.sleep(100.millis) >> wait)
                 wait
               }
      } yield got shouldBe Vector("orphan")).timeout(60.seconds)
    }
  }
}
