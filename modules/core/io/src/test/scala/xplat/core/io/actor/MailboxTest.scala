package xplat.core.io.actor

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Deferred, IO, Ref}
import cats.implicits._
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api._
import xplat.core.api.error.MailboxOverflow
import xplat.core.io.actor.ActorTestKit._

import scala.concurrent.duration._

class MailboxTest extends AsyncFreeSpec with AsyncIOSpec with PlatformAsyncSpec with Matchers {
  import MailboxTest._

  "bounded mailbox" - {

    "DropNew keeps the waiting messages and discards the new ones" in withSystem { system =>
      for {
        probe <- Probe(system, Mailbox.bounded(2, Mailbox.Overflow.DropNew))
        _     <- (1 to 4).toList.traverse_(probe.ref ! _)
        seen  <- probe.release(3)
      } yield seen shouldBe List(0, 1, 2)
    }

    "DropOldest discards the oldest waiting messages to make room" in withSystem { system =>
      for {
        probe <- Probe(system, Mailbox.bounded(2, Mailbox.Overflow.DropOldest))
        _     <- (1 to 4).toList.traverse_(probe.ref ! _)
        seen  <- probe.release(3)
      } yield seen shouldBe List(0, 3, 4)
    }

    "Fail rejects the new message with MailboxOverflow" in withSystem { system =>
      for {
        probe  <- Probe(system, Mailbox.bounded(2, Mailbox.Overflow.Fail))
        _      <- (probe.ref ! 1) >> (probe.ref ! 2)
        result <- (probe.ref ! 3).attempt
        seen   <- probe.release(3)
      } yield {
        result shouldBe Left(MailboxOverflow("xplat/test/probe", 2))
        seen shouldBe List(0, 1, 2)
      }
    }

    "accepts messages again once the actor catches up" in withSystem { system =>
      for {
        probe <- Probe(system, Mailbox.bounded(1, Mailbox.Overflow.Fail))
        _     <- probe.ref ! 1
        full  <- (probe.ref ! 2).attempt
        _     <- probe.release(2)
        _     <- probe.ref ! 3
        seen  <- awaitUntil(probe.seen.get)(_.size == 3)
      } yield {
        full.isLeft shouldBe true
        seen shouldBe List(0, 1, 3)
      }
    }

    "messages the actor pipes to itself are not limited and keep their order" in withSystem { system =>
      for {
        probe <- Probe(system, Mailbox.bounded(1, Mailbox.Overflow.Fail))
        _     <- probe.ref ! 1
        ctx   <- probe.context.get
        _     <- ctx.pipe(7) >> ctx.pipe(8)
        seen  <- probe.release(4)
      } yield seen shouldBe List(0, 1, 7, 8)
    }

    "stop is accepted while the mailbox is full" in withSystem { system =>
      for {
        probe <- Probe(system, Mailbox.bounded(1, Mailbox.Overflow.Fail))
        _     <- probe.ref ! 1
        _     <- system.spawn[Int]("other")(Behaviors.same) // the system keeps working
        _     <- probe.context.get.flatMap(_.stop())
        _     <- probe.gate.complete(())
        _     <- probe.ref.terminated
      } yield succeed
    }
  }

  "ActorStream" - {

    "source emits the messages sent to its ref, in order, until completed" in {
      for {
        source <- ActorStream.source[IO, Int]("numbers")
        _      <- (1 to 3).toList.traverse_(source.ref ! _)
        _      <- source.complete
        out    <- source.stream.compile.toList
        late   <- (source.ref ! 4).attempt
      } yield {
        out shouldBe List(1, 2, 3)
        late.isLeft shouldBe true
      }
    }

    "source applies the mailbox policy while the stream is not pulling" in {
      for {
        source <- ActorStream.source[IO, Int]("bounded", Mailbox.bounded(3, Mailbox.Overflow.DropNew))
        _      <- (1 to 5).toList.traverse_(source.ref ! _)
        _      <- source.complete
        out    <- source.stream.compile.toList
      } yield out shouldBe List(1, 2, 3)
    }

    "source feeds a running stream" in {
      for {
        source <- ActorStream.source[IO, Int]("live")
        fiber  <- source.stream.compile.toList.start
        _      <- (1 to 100).toList.traverse_(source.ref ! _)
        _      <- source.complete
        out    <- fiber.joinWithNever.timeout(Timeout)
      } yield out shouldBe (1 to 100).toList
    }

    "sink sends every element to the actor" in withSystem { system =>
      for {
        ref   <- system.spawn[Sum]("sum")(sum(0))
        _     <- Stream.range(1, 11).covary[IO].map(n => Add(n): Sum).through(ActorStream.sink(ref)).compile.drain
        total <- ref.ask[Int](Total)
      } yield total shouldBe 55
    }

    "ask waits for each answer and keeps the order" in withSystem { system =>
      for {
        inFlight <- Ref.of[IO, (Int, Int)]((0, 0)) // (current, max)
        ref <- system.spawn[Double]("double")(Behaviors.message { case Double(n, replyTo) =>
                   inFlight.update { case (c, m) => (c + 1, m max (c + 1)) } >>
                     IO.sleep(10.millis) >>
                     inFlight.update { case (c, m) => (c - 1, m) } >>
                     IO(replyTo ! n * 2)
                 })
        out    <- Stream.range(1, 6).covary[IO].through(ActorStream.ask(ref, parallelism = 2)((n: Int, r: ReplyTo[Int]) => Double(n, r))).compile.toList
        maxRun <- inFlight.get.map(_._2)
      } yield {
        out shouldBe List(2, 4, 6, 8, 10)
        maxRun shouldBe 1 // the actor still handles one message at a time
      }
    }
  }
}

object MailboxTest {

  /** An actor that records every message and blocks on its first one (0) until `gate` opens, so the mailbox fills up. */
  final case class Probe(ref: ActorRef[IO, Int], seen: Ref[IO, List[Int]], gate: Deferred[IO, Unit], context: Deferred[IO, Context[IO, Int]]) {

    /** Opens the gate and waits until the actor has seen `n` messages, then a little longer to catch extra ones. */
    def release(n: Int): IO[List[Int]] =
      gate.complete(()) >> awaitUntil(seen.get)(_.size >= n) >> IO.sleep(100.millis) >> seen.get
  }

  object Probe {
    def apply(system: ActorSystem[IO], mailbox: Mailbox): IO[Probe] =
      for {
        seen    <- Ref.of[IO, List[Int]](Nil)
        gate    <- Deferred[IO, Unit]
        context <- Deferred[IO, Context[IO, Int]]
        ref <- system.spawn[Int]("probe", mailbox)(Behaviors.setup { ctx =>
                   Behaviors.pipe(context.complete(ctx).as(Behaviors.message { m =>
                     seen.update(_ :+ m) >> (if (m == 0) gate.get else IO.unit)
                   }))
                 })
        _ <- ref ! 0
        _ <- awaitUntil(seen.get)(_ == List(0)) // 0 is being handled, so the mailbox is empty
      } yield Probe(ref, seen, gate, context)
  }

  sealed trait Sum
  final case class Add(n: Int) extends Sum
  final case class Total(replyTo: ReplyTo[Int]) extends Sum

  def sum(n: Int): Behavior[IO, Sum] =
    Behaviors.message {
      case Add(x) => sum(n + x)
      case Total(replyTo) =>
        replyTo ! n
        Behaviors.same
    }

  final case class Double(n: Int, replyTo: ReplyTo[Int])

}
