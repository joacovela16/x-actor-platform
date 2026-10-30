package xplat.core.io.actor

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Deferred, IO, Ref => CRef}
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
      system.spawn[Int]("a1")(b => b.same).map { ref =>
        (ref.name, ref.path) shouldBe ("a1", "xplat/test/a1")
      }
    }

    "the system exposes its name" in withSystem { system =>
      IO(system.name shouldBe "test")
    }

    "terminated completes when the actor stops, and the name can be reused" in withSystem { system =>
      for {
        first  <- system.spawn[Int]("reused")(b => b.message(_ => b.stop))
        _      <- first ! 1
        _      <- first.terminated
        second <- system.spawn[Int]("reused")(b => b.same)
      } yield second.path shouldBe "xplat/test/reused"
    }

    "terminated does not complete while the actor runs" in withSystem { system =>
      for {
        ref    <- system.spawn[Int]("alive")(b => b.message(_ => b.same))
        result <- ref.terminated.timeout(200.millis).attempt
      } yield result.isLeft shouldBe true
    }

    "rejects a second actor with the same name" in withSystem { system =>
      for {
        _      <- system.spawn[Int]("dup")(b => b.same)
        result <- system.spawn[Int]("dup")(b => b.same).attempt
      } yield result.left.map(_.getMessage) shouldBe Left("children dup exists")
    }

    "runs the initial effect of a piped behavior when the actor starts" in withSystem { system =>
      for {
        started <- Deferred[IO, Unit]
        _       <- system.spawn[Int]("piped")(b => b.pipe(started.complete(()).as(b.same)))
        _       <- started.get
      } yield succeed
    }

    "runs a context setup only once" in withSystem { system =>
      for {
        setups <- CRef.of[IO, Int](0)
        seen   <- CRef.of[IO, Vector[Int]](Vector.empty)
        ref <- system.spawn[Int]("setup") { b =>
          b.context(_ => setups.update(_ + 1).as(b.message(n => seen.update(_ :+ n))))
        }
        _     <- List(1, 2, 3).traverse_(ref ! _)
        _     <- awaitUntil(seen.get)(_.size == 3)
        count <- setups.get
      } yield count shouldBe 1
    }
  }

  "messages" - {

    "are delivered in the order they were sent" in withSystem { system =>
      for {
        seen <- CRef.of[IO, Vector[Int]](Vector.empty)
        ref  <- system.spawn[Int]("ordered")(b => b.message(n => seen.update(_ :+ n)))
        _    <- (1 to 20).toList.traverse_(ref ! _)
        xs   <- awaitUntil(seen.get)(_.size == 20)
      } yield xs shouldBe (1 to 20).toVector
    }

    "an actor can send a message to itself from its handler" in withSystem { system =>
      for {
        seen <- CRef.of[IO, Vector[Int]](Vector.empty)
        ref <- system.spawn[Int]("self-send") { b =>
          b.context(c => b.message(n => seen.update(_ :+ n) >> (if (n < 3) c.pipe(n + 1) else IO.unit)))
        }
        _  <- ref ! 1
        xs <- awaitUntil(seen.get)(_.size == 3)
      } yield xs shouldBe Vector(1, 2, 3)
    }

    "can switch the behavior to carry state between messages" in withSystem { system =>
      for {
        ref   <- system.spawn[CounterMsg]("counter")(b => counter(b, 0))
        _     <- List.fill(3)(ref ! Inc).sequence_
        value <- ref.ask[Int](Get(_))
      } yield value shouldBe 3
    }
  }

  "ask" - {

    "returns the value the actor replies with" in withSystem { system =>
      for {
        ref   <- system.spawn[Echo]("echo")(b => b.message { case Echo(text, replyTo) => replyTo ! text.toUpperCase })
        reply <- ref ? (Echo("hola", _))
      } yield reply shouldBe "HOLA"
    }

    "serves several asks in a row" in withSystem { system =>
      for {
        ref     <- system.spawn[Echo]("echo")(b => b.message { case Echo(text, replyTo) => replyTo ! text })
        replies <- List("a", "b", "c").traverse(t => ref.ask[String](Echo(t, _)))
      } yield replies shouldBe List("a", "b", "c")
    }

    "fails with a timeout when the actor never replies" in withSystem { system =>
      for {
        ref    <- system.spawn[CounterMsg]("silent")(b => b.message(_ => b.same))
        result <- ref.ask[Int](Get(_), 100.millis).attempt
      } yield result.left.map(_.getMessage) shouldBe Left("Ask timeout")
    }
  }

  "lifecycle" - {

    "an actor that returns stop rejects further messages" in withSystem { system =>
      for {
        ref   <- system.spawn[Int]("stopper")(b => b.message(n => if (n == 0) b.stop else b.same))
        _     <- ref ! 0
        error <- awaitStopped(ref ! 1)
      } yield error.getMessage shouldBe "Actor xplat/test/stopper already stopped"
    }

    "terminating the system stops its actors" in {
      IOActorSystem[IO]("test").allocated.flatMap { case (system, release) =>
        for {
          ref    <- system.spawn[Int]("a1")(b => b.message(_ => b.same))
          _      <- release.timeout(Timeout)
          result <- (ref ! 1).attempt
        } yield result.isLeft shouldBe true
      }
    }
  }

  "errors" - {

    "an exception without a signal handler stops the actor" in withSystem { system =>
      for {
        ref <- system.spawn[Int]("fragile")(b => b.message(_ => throw Boom))
        _   <- ref ! 1
        _   <- awaitStopped(ref ! 2)
      } yield succeed
    }

    "a failed effect without a signal handler stops the actor" in withSystem { system =>
      for {
        ref <- system.spawn[Int]("fragile")(b => b.message(_ => IO.raiseError[Unit](Boom)))
        _   <- ref ! 1
        _   <- awaitStopped(ref ! 2)
      } yield succeed
    }

    "OnError handler receives the exception and the actor keeps working" in withSystem { system =>
      for {
        errors <- CRef.of[IO, Vector[String]](Vector.empty)
        seen   <- CRef.of[IO, Vector[Int]](Vector.empty)
        ref <- system.spawn[Int]("resilient") { b =>
          b.message(n => if (n == 0) throw Boom else seen.update(_ :+ n))
            .onSignal { case Signal.OnError(e) => errors.update(_ :+ e.getMessage) }
        }
        _   <- ref ! 0
        _   <- ref ! 1
        xs  <- awaitUntil(seen.get)(_.nonEmpty)
        err <- errors.get
      } yield (err, xs) shouldBe (Vector(Boom.getMessage), Vector(1))
    }

    "OnError handler is called when the setup fails" in withSystem { system =>
      for {
        caught <- Deferred[IO, Throwable]
        _ <- system.spawn[Int]("bad-setup") { b =>
          b.context(_ => throw Boom).onSignal { case Signal.OnError(e) => caught.complete(e).as(b.stop) }
        }
        error <- caught.get
      } yield error shouldBe Boom
    }
  }

  "signals" - {

    "a signal sent through the context reaches the onSignal handler" in withSystem { system =>
      for {
        ctx      <- Deferred[IO, Context[IO, Int]]
        received <- Deferred[IO, Signal]
        _ <- system.spawn[Int]("signals") { b =>
          b.context { c =>
            ctx.complete(c).as {
              b.message(_ => b.same).onSignal { case s @ Signal.Kill => received.complete(s).void }
            }
          }
        }
        c      <- ctx.get
        _      <- c.signal(Signal.Kill)
        signal <- received.get
      } yield signal shouldBe Signal.Kill
    }

    "the actor receives PostStop when it stops" in withSystem { system =>
      for {
        postStop <- Deferred[IO, Unit]
        ref <- system.spawn[Int]("post-stop") { b =>
          b.message(_ => b.stop).onSignal { case Signal.PostStop => postStop.complete(()).void }
        }
        _ <- ref ! 1
        _ <- postStop.get
      } yield succeed
    }
  }

  "children" - {

    "can be spawned from outside through the parent context" in withSystem { system =>
      for {
        ctx   <- Deferred[IO, Context[IO, Int]]
        _     <- system.spawn[Int]("parent")(b => b.context(c => ctx.complete(c).as(b.message(_ => b.same))))
        c     <- ctx.get
        child <- c.spawn[Int]("child")(b => b.same)
      } yield child.path shouldBe "xplat/test/parent/child"
    }

    "can be spawned from inside a message handler" in withSystem { system =>
      for {
        spawned <- Deferred[IO, String]
        parent <- system.spawn[Int]("parent") { b =>
          b.context { c =>
            b.message(_ => c.spawn[Int]("child")(cb => cb.same).flatMap(ref => spawned.complete(ref.path)).void)
          }
        }
        _    <- parent ! 1
        path <- spawned.get
      } yield path shouldBe "xplat/test/parent/child"
    }

    "stopChild stops only that child" in withSystem { system =>
      for {
        ctx    <- Deferred[IO, Context[IO, Int]]
        parent <- system.spawn[Int]("parent")(b => b.context(c => ctx.complete(c).as(b.message(_ => b.same))))
        c      <- ctx.get
        child  <- c.spawn[Int]("child")(b => b.message(_ => b.same))
        _      <- c.stopChild("child")
        _      <- awaitStopped(child ! 1)
        alive  <- (parent ! 1).attempt
      } yield alive shouldBe Right(())
    }

    "stopping the parent stops its children" in withSystem { system =>
      for {
        ctx    <- Deferred[IO, Context[IO, Int]]
        parent <- system.spawn[Int]("parent")(b => b.context(c => ctx.complete(c).as(b.message(_ => b.stop))))
        c      <- ctx.get
        child  <- c.spawn[Int]("child")(b => b.message(_ => b.same))
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
        ran    <- CRef.of[IO, Boolean](false)
        cancel <- system.schedule(ran.set(true), 100.millis)
        _      <- cancel()
        _      <- IO.sleep(300.millis)
        value  <- ran.get
      } yield value shouldBe false
    }

    "terminating the system cancels pending scheduled tasks" in {
      IOActorSystem[IO]("test").allocated.flatMap { case (system, release) =>
        for {
          ran   <- CRef.of[IO, Boolean](false)
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
        _    <- system.spawn[Int]("scheduler")(b => b.context(c => c.schedule(done.complete(()), 50.millis).as(b.same)))
        _    <- done.get
      } yield succeed
    }

    "a timer behavior delivers its message after the delay" in withSystem { system =>
      for {
        got <- Deferred[IO, Int]
        _ <- system.spawn[Int]("timer") { b =>
          b.timer(50.millis, 42)(_ => b.message(n => got.complete(n).void))
        }
        n <- got.get
      } yield n shouldBe 42
    }
  }

  "stash" - {

    "unstashAll replays the stashed messages in order" in withSystem { system =>
      for {
        seen <- CRef.of[IO, Vector[String]](Vector.empty)
        ref <- system.spawn[String]("stash") { b =>
          b.stash(10) { stash =>
            val open: Behavior[IO, String] = b.message(s => seen.update(_ :+ s))
            b.message {
              case "open" => stash.unstashAll(open)
              case other  => stash.push(other)
            }
          }
        }
        _  <- List("a", "b", "open").traverse_(ref ! _)
        xs <- awaitUntil(seen.get)(_.size == 2)
      } yield xs shouldBe Vector("a", "b")
    }
  }
}

object IOActorSystemTest {

  case object Boom extends RuntimeException("boom")

  sealed trait CounterMsg
  case object Inc extends CounterMsg
  final case class Get(replyTo: Ref[Int]) extends CounterMsg

  final case class Echo(text: String, replyTo: Ref[String])

  def counter(b: Behaviors[IO, CounterMsg], n: Int): Behavior[IO, CounterMsg] =
    b.message {
      case Inc => counter(b, n + 1)
      case Get(replyTo) =>
        replyTo ! n
        b.same
    }
}
