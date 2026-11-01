package xplat.core.connectors

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource, Ref => CRef}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api.{ActorSystem, Ref}
import xplat.core.io.actor.IOActorSystem

import java.util.UUID
import scala.concurrent.duration._

/** Behaviour every connector must have, run against a real broker. */
abstract class ConnectorContract extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  /** Set to false to cancel the tests, e.g. when the broker cannot be started. */
  def available: Boolean = true

  def publisher(destination: String): Resource[IO, Publisher[IO, String]]

  /** A consumer of `destination`. Consumers with the same `group` share the messages and the acknowledgements. */
  def consumer(destination: String, group: String, retry: RetryPolicy)(sink: Sink[IO, String]): Resource[IO, RunningConsumer[IO]]

  protected val timeout: FiniteDuration = 90.seconds
  private val fast = RetryPolicy(maxRetries = 3, initialBackoff = 10.millis, maxBackoff = 50.millis)

  private def fresh(prefix: String): String = s"$prefix-${UUID.randomUUID().toString.take(8)}"

  private def eventually[A](io: IO[A])(p: A => Boolean): IO[A] =
    io.flatMap(a => if (p(a)) IO.pure(a) else IO.sleep(100.millis) >> eventually(io)(p))

  private final case class Got(message: Incoming[String], replyTo: Ref[Done])

  /** An actor that keeps the values it receives and confirms them. */
  private def collector(system: ActorSystem[IO], received: CRef[IO, Vector[Incoming[String]]]) =
    system.spawn[Got](fresh("collector"))(b => b.message { case Got(m, replyTo) => received.update(_ :+ m).map(_ => replyTo ! Done) })

  private def checked(test: => IO[org.scalatest.Assertion]): IO[org.scalatest.Assertion] =
    IO(assume(available, "the broker is not available")) >> test.timeout(timeout)

  "a connector" - {

    "delivers to an actor what a publisher actor publishes, in order, with key and headers" in checked {
      val destination = fresh("orders")
      IOActorSystem[IO]("connectors").use { system =>
        for {
          received <- CRef[IO].of(Vector.empty[Incoming[String]])
          sinkRef  <- collector(system, received)
          values    = (1 to 10).map(i => s"m$i").toList
          results <- (publisher(destination), consumer(destination, fresh("group"), fast)(Sink.actor(sinkRef)(Got(_, _)))).tupled.use { case (pub, _) =>
                       Publisher.spawn(system, fresh("publisher"), pub).flatMap { ref =>
                         values.traverse(v => ref.ask[Publisher.Result](Publisher.Publish.confirmed(Outgoing(v, Some("k"), Map("origin" -> "test"))))) <*
                           eventually(received.get)(_.size >= values.size)
                       }
                     }
          got <- received.get
        } yield {
          results.distinct shouldBe List(Publisher.Result.Published)
          got.map(_.value) shouldBe values
          got.map(_.key).distinct shouldBe Vector(Some("k"))
          got.map(_.headers.get("origin")).distinct shouldBe Vector(Some("test"))
          got.map(_.source).distinct shouldBe Vector(destination)
        }
      }
    }

    "retries a message whose delivery fails, without losing the order" in checked {
      val destination = fresh("retries")
      for {
        attempts <- CRef[IO].of(0)
        received <- CRef[IO].of(Vector.empty[String])
        sink = Sink[IO, String] { m =>
                 if (m.value == "boom") attempts.updateAndGet(_ + 1).flatMap(n => IO.raiseError(new RuntimeException("not yet")).whenA(n < 3)) >> received.update(_ :+ m.value)
                 else received.update(_ :+ m.value)
               }
        got <- (publisher(destination), consumer(destination, fresh("group"), fast)(sink)).tupled.use { case (pub, _) =>
                 List("a", "boom", "b").traverse_(pub.publish(_)) >> eventually(received.get)(_.size >= 3)
               }
        n <- attempts.get
      } yield (got, n) shouldBe (Vector("a", "boom", "b"), 3)
    }

    "a message that cannot be processed stops the consumer and is delivered again to the next one" in checked {
      val destination = fresh("redelivery")
      val group = fresh("group")
      val failing = Sink[IO, String](_ => IO.raiseError(new RuntimeException("cannot process")))
      for {
        received <- CRef[IO].of(Vector.empty[String])
        stopped <- publisher(destination).use { pub =>
                     pub.publish("first") >> pub.publish("second") >>
                       consumer(destination, group, fast.copy(maxRetries = 0))(failing).use(_.stopped.attempt)
                   }
        got <- consumer(destination, group, fast)(Sink[IO, String](m => received.update(_ :+ m.value))).use { _ =>
                 eventually(received.get)(_.size >= 2)
               }
      } yield {
        stopped.left.map(_.getClass) shouldBe Left(classOf[Consuming.ConsumerStopped])
        got.take(2) shouldBe Vector("first", "second")
      }
    }

    "with Skip, a message that cannot be processed is dropped and the next ones go on" in checked {
      val destination = fresh("skips")
      for {
        received <- CRef[IO].of(Vector.empty[String])
        sink = Sink[IO, String](m => if (m.value == "bad") IO.raiseError(new RuntimeException("bad")) else received.update(_ :+ m.value))
        got <- (publisher(destination), consumer(destination, fresh("group"), fast.copy(whenExhausted = RetryPolicy.Skip))(sink)).tupled.use { case (pub, _) =>
                 List("a", "bad", "b").traverse_(pub.publish(_)) >> eventually(received.get)(_.size >= 2)
               }
      } yield got shouldBe Vector("a", "b")
    }
  }
}
