package xplat.core.io.actor

import cats.Monad
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Deferred, IO, Ref}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api._
import xplat.core.io.actor.ActorTestKit._

import scala.concurrent.duration._

class IOActorSystemTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import IOActorSystemTest._

  "spawn" - {

    "exposes the actor name and its hierarchical path" in withSystem { system =>
      system.spawn[Int]("a1")(Behaviors.same).map { ref =>
        (ref.name, ref.path) shouldBe ("a1", "xplat/test/a1")
      }
    }

    "the system exposes its name" in withSystem { system =>
      IO(system.name shouldBe "test")
    }

    "terminated completes when the actor stops, and the name can be reused" in withSystem { system =>
      for {
        first  <- system.spawn[Int]("reused")(Behaviors.message(_ => Behaviors.stop))
        _      <- first ! 1
        _      <- first.terminated
        second <- system.spawn[Int]("reused")(Behaviors.same)
      } yield second.path shouldBe "xplat/test/reused"
    }

    "terminated does not complete while the actor runs" in withSystem { system =>
      for {
        ref    <- system.spawn[Int]("alive")(Behaviors.message(_ => Behaviors.same))
        result <- ref.terminated.timeout(200.millis).attempt
      } yield result.isLeft shouldBe true
    }

    "a spawn cancelled during the setup frees the name and does not block the shutdown" in withSystem { system =>
      for {
        started <- Deferred[IO, Unit]
        fiber   <- system.spawn[Int]("slow")(Behaviors.pipe(started.complete(()) >> IO.never[Behavior[IO, Int]])).start
        _       <- started.get
        _       <- fiber.cancel
        outcome <- fiber.join
        again   <- awaitUntil(system.spawn[Int]("slow")(Behaviors.same).attempt)(_.isRight)
      } yield {
        outcome.isCanceled shouldBe true
        again.map(_.path) shouldBe Right("xplat/test/slow")
      }
    }

    "rejects a second actor with the same name" in withSystem { system =>
      for {
        _      <- system.spawn[Int]("dup")(Behaviors.same)
        result <- system.spawn[Int]("dup")(Behaviors.same).attempt
      } yield result.left.map(_.getMessage) shouldBe Left("children dup exists")
    }

    "runs the initial effect of a piped behavior when the actor starts" in withSystem { system =>
      for {
        started <- Deferred[IO, Unit]
        _       <- system.spawn[Int]("piped")(started.complete(()).void)
        _       <- started.get
      } yield succeed
    }

    "runs a context setup only once" in withSystem { system =>
      for {
        setups <- Ref.of[IO, Int](0)
        seen   <- Ref.of[IO, Vector[Int]](Vector.empty)
        ref <- system.spawn[Int]("setup") ( Behaviors.setup(_ => Behaviors.pipe(setups.update(_ + 1).as(Behaviors.message(n => seen.update(_ :+ n))))))
        _     <- List(1, 2, 3).traverse_(ref ! _)
        _     <- awaitUntil(seen.get)(_.size == 3)
        count <- setups.get
      } yield count shouldBe 1
    }
  }

  "messages" - {

    "are delivered in the order they were sent" in withSystem { system =>
      for {
        seen <- Ref.of[IO, Vector[Int]](Vector.empty)
        ref  <- system.spawn[Int]("ordered")(Behaviors.message(n => seen.update(_ :+ n)))
        _    <- (1 to 20).toList.traverse_(ref ! _)
        xs   <- awaitUntil(seen.get)(_.size == 20)
      } yield xs shouldBe (1 to 20).toVector
    }

    "an actor can send a message to itself from its handler" in withSystem { system =>
      for {
        seen <- Ref.of[IO, Vector[Int]](Vector.empty)
        ref <- system.spawn[Int]("self-send") ( Behaviors.setup(c => Behaviors.message(n => seen.update(_ :+ n) >> (if (n < 3) c.pipe(n + 1) else IO.unit))))
        _  <- ref ! 1
        xs <- awaitUntil(seen.get)(_.size == 3)
      } yield xs shouldBe Vector(1, 2, 3)
    }

    "can switch the behavior to carry state between messages" in withSystem { system =>
      for {
        ref   <- system.spawn[CounterMsg]("counter")(counter(0))
        _     <- List.fill(3)(ref ! Inc).sequence_
        value <- ref.ask[Int](Get(_))
      } yield value shouldBe 3
    }
  }

  "ask" - {

    "returns the value the actor replies with" in withSystem { system =>
      for {
        ref   <- system.spawn[Echo]("echo")(Behaviors.message { case Echo(text, replyTo) => replyTo ! text.toUpperCase })
        reply <- ref ? (Echo("hola", _))
      } yield reply shouldBe "HOLA"
    }

    "serves several asks in a row" in withSystem { system =>
      for {
        ref     <- system.spawn[Echo]("echo")(Behaviors.message { case Echo(text, replyTo) => replyTo ! text })
        replies <- List("a", "b", "c").traverse(t => ref.ask[String](Echo(t, _)))
      } yield replies shouldBe List("a", "b", "c")
    }

    "fails with a timeout when the actor never replies" in withSystem { system =>
      for {
        ref    <- system.spawn[CounterMsg]("silent")(Behaviors.message(_ => Behaviors.same))
        result <- ref.ask[Int](Get(_), 100.millis).attempt
      } yield result.left.map(_.getMessage) shouldBe Left("Ask timeout")
    }
  }

  "lifecycle" - {

    "an actor that returns stop rejects further messages" in withSystem { system =>
      for {
        ref   <- system.spawn[Int]("stopper")(Behaviors.message(n => if (n == 0) Behaviors.stop else Behaviors.same))
        _     <- ref ! 0
        error <- awaitStopped(ref ! 1)
      } yield error.getMessage shouldBe "Actor xplat/test/stopper already stopped"
    }

    "terminating the system stops its actors" in {
      IOActorSystem[IO]("test").allocated.flatMap { case (system, release) =>
        for {
          ref    <- system.spawn[Int]("a1")(Behaviors.message(_ => Behaviors.same))
          _      <- release.timeout(Timeout)
          result <- (ref ! 1).attempt
        } yield result.isLeft shouldBe true
      }
    }
  }

  "errors" - {

    "an exception without a signal handler stops the actor" in withSystem { system =>
      for {
        ref <- system.spawn[Int]("fragile")(Behaviors.message(_ => throw Boom))
        _   <- ref ! 1
        _   <- awaitStopped(ref ! 2)
      } yield succeed
    }

    "a failed effect without a signal handler stops the actor" in withSystem { system =>
      for {
        ref <- system.spawn[Int]("fragile")(Behaviors.message(_ => IO.raiseError[Unit](Boom)))
        _   <- ref ! 1
        _   <- awaitStopped(ref ! 2)
      } yield succeed
    }

    "OnError handler receives the exception and the actor keeps working" in withSystem { system =>
      for {
        errors <- Ref.of[IO, Vector[String]](Vector.empty)
        seen   <- Ref.of[IO, Vector[Int]](Vector.empty)
        ref <- system.spawn[Int]("resilient") ( Behaviors.onSignal(
          Behaviors.message(n => if (n == 0) throw Boom else seen.update(_ :+ n)),
          { case Signal.OnError(e) => errors.update(_ :+ e.getMessage) }
        ))
        _   <- ref ! 0
        _   <- ref ! 1
        xs  <- awaitUntil(seen.get)(_.nonEmpty)
        err <- errors.get
      } yield (err, xs) shouldBe (Vector(Boom.getMessage), Vector(1))
    }

    "OnError handler is called when the setup fails" in withSystem { system =>
      for {
        caught <- Deferred[IO, Throwable]
        _ <- system.spawn[Int]("bad-setup") ( Behaviors.onSignal(
          Behaviors.setup(_ => throw Boom),
          { case Signal.OnError(e) => Behaviors.pipe(caught.complete(e).as(Behaviors.stop)) }
        ))
        error <- caught.get
      } yield error shouldBe Boom
    }
  }

  "signals" - {

    "a signal sent through the context reaches the onSignal handler" in withSystem { system =>
      for {
        ctx      <- Deferred[IO, Context[IO, Int]]
        received <- Deferred[IO, Signal]
        _ <- system.spawn[Int]("signals") ( Behaviors.setup { c =>
            Behaviors.pipe(ctx.complete(c).as {
              Behaviors.onSignal(Behaviors.message(_ => Behaviors.same), { case s @ Signal.Kill => received.complete(s).void })
            })
          })
        c      <- ctx.get
        _      <- c.signal(Signal.Kill)
        signal <- received.get
      } yield signal shouldBe Signal.Kill
    }

    "the actor receives PostStop when it stops" in withSystem { system =>
      for {
        postStop <- Deferred[IO, Unit]
        ref <- system.spawn[Int]("post-stop") ( Behaviors.onSignal(Behaviors.message(_ => Behaviors.stop), { case Signal.PostStop => postStop.complete(()).void }))
        _ <- ref ! 1
        _ <- postStop.get
      } yield succeed
    }
  }

  "children" - {

    "can be spawned from outside through the parent context" in withSystem { system =>
      for {
        ctx   <- Deferred[IO, Context[IO, Int]]
        _     <- system.spawn[Int]("parent")(Behaviors.setup(c => Behaviors.pipe(ctx.complete(c).as(Behaviors.message(_ => Behaviors.same)))))
        c     <- ctx.get
        child <- c.spawn[Int]("child")(Behaviors.same)
      } yield child.path shouldBe "xplat/test/parent/child"
    }

    "can be spawned from inside a message handler" in withSystem { system =>
      for {
        spawned <- Deferred[IO, String]
        parent <- system.spawn[Int]("parent") ( Behaviors.setup { c =>
            Behaviors.message(_ => c.spawn[Int]("child")(Behaviors.same).flatMap(ref => spawned.complete(ref.path)).void)
          })
        _    <- parent ! 1
        path <- spawned.get
      } yield path shouldBe "xplat/test/parent/child"
    }

    "stopChild stops only that child" in withSystem { system =>
      for {
        ctx    <- Deferred[IO, Context[IO, Int]]
        parent <- system.spawn[Int]("parent")(Behaviors.setup(c => Behaviors.pipe(ctx.complete(c).as(Behaviors.message(_ => Behaviors.same)))))
        c      <- ctx.get
        child  <- c.spawn[Int]("child")(Behaviors.message(_ => Behaviors.same))
        _      <- c.stopChild("child")
        _      <- awaitStopped(child ! 1)
        alive  <- (parent ! 1).attempt
      } yield alive shouldBe Right(())
    }

    "stopping the parent stops its children" in withSystem { system =>
      for {
        ctx    <- Deferred[IO, Context[IO, Int]]
        parent <- system.spawn[Int]("parent")(Behaviors.setup(c => Behaviors.pipe(ctx.complete(c).as(Behaviors.message(_ => Behaviors.stop)))))
        c      <- ctx.get
        child  <- c.spawn[Int]("child")(Behaviors.message(_ => Behaviors.same))
        _      <- parent ! 1
        _      <- awaitStopped(child ! 1)
        _      <- awaitStopped(parent ! 1)
      } yield succeed
    }
  }

  "scheduling" - {

    "system.schedule runs the effect after the delay" in withSystem { system =>
      for {
        done <- Deferred[IO, Unit]
        _    <- system.schedule(done.complete(()), 50.millis)
        _    <- done.get
      } yield succeed
    }

    "a cancelled schedule never runs" in withSystem { system =>
      for {
        ran    <- Ref.of[IO, Boolean](false)
        cancel <- system.schedule(ran.set(true), 100.millis)
        _      <- cancel()
        _      <- IO.sleep(300.millis)
        value  <- ran.get
      } yield value shouldBe false
    }

    "terminating the system cancels pending scheduled tasks" in {
      IOActorSystem[IO]("test").allocated.flatMap { case (system, release) =>
        for {
          ran   <- Ref.of[IO, Boolean](false)
          _     <- system.schedule(ran.set(true), 200.millis)
          _     <- system.schedule(IO.unit, 1.hour) // must not delay the shutdown
          _     <- release.timeout(Timeout)
          _     <- IO.sleep(400.millis)
          value <- ran.get
        } yield value shouldBe false
      }
    }

    "a task scheduled from an actor context runs after the delay" in withSystem { system =>
      for {
        done <- Deferred[IO, Unit]
        _    <- system.spawn[Int]("scheduler")(Behaviors.setup(c => c.schedule(done.complete(()), 50.millis).void))
        _    <- done.get
      } yield succeed
    }

    "a timer behavior delivers its message after the delay" in withSystem { system =>
      for {
        got <- Deferred[IO, Int]
        _ <- system.spawn[Int]("timer") ( Behaviors.timer(50.millis, 42)(_ => Behaviors.message(n => got.complete(n).void)))
        n <- got.get
      } yield n shouldBe 42
    }
  }

  "stash" - {

    "unstashAll replays the stashed messages in order" in withSystem { system =>
      for {
        seen <- Ref.of[IO, Vector[String]](Vector.empty)
        ref <- system.spawn[String]("stash") ( Behaviors.stash(10) { stash =>
            val open: Behavior[IO, String] = Behaviors.message(s => seen.update(_ :+ s))
            Behaviors.message {
              case "open" => stash.unstashAll(open)
              case other  => stash.push(other)
            }
          })
        _  <- List("a", "b", "open").traverse_(ref ! _)
        xs <- awaitUntil(seen.get)(_.size == 2)
      } yield xs shouldBe Vector("a", "b")
    }
  }

  "behaviors written for any F[_]" - {

    "are built without naming F, and run on IO" in withSystem { system =>
      for {
        ref   <- system.spawn[CounterMsg]("generic")(genericCounter[IO](0))
        _     <- ref ! Inc
        _     <- ref ! Inc
        count <- ref.ask[Int](Get(_))
      } yield count shouldBe 2
    }
  }
}

object IOActorSystemTest {

  case object Boom extends RuntimeException("boom")

  sealed trait CounterMsg
  case object Inc extends CounterMsg
  final case class Get(replyTo: ReplyTo[Int]) extends CounterMsg

  final case class Echo(text: String, replyTo: ReplyTo[String])

  // Only `Monad[F]`: no IO anywhere. F is inferred from the declared result type.
  def genericCounter[F[_]](n: Int)(implicit F: Monad[F]): Behavior[F, CounterMsg] =
    Behaviors.setup { ctx =>
      Behaviors.message {
        case Inc => Behaviors.pipe(F.pure(genericCounter[F](n + 1)))
        case Get(replyTo) => ctx.log.debug(s"get $n") >> F.pure(replyTo ! n) // F[Unit] keeps the behavior
      }
    }

  def counter(n: Int): Behavior[IO, CounterMsg] =
    Behaviors.message {
      case Inc => counter(n + 1)
      case Get(replyTo) =>
        replyTo ! n
        Behaviors.same
    }
}
