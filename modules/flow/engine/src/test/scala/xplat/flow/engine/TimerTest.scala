package xplat.flow.engine

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.circe.Json
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine.AuditEntry.{Decision, NodeFinished, NodeWaiting}
import xplat.flow.engine.InstanceState.{NodeStatus, Status}
import xplat.flow.model._
import xplat.flow.nodes.NodeType

import scala.concurrent.duration._

/** Nodes that wait until a time: the generic suspension of the engine, and the cron node that uses it. */
class TimerTest extends EngineSpec {

  /** A node that answers that it goes on `ms` milliseconds after it started, writing `v`, and counts how many times it ran. */
  private final class Waits(runs: Ref[IO, Int]) extends NodeType[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor("test.wait", "Test", "Wait", ConfigSchema(List(ConfigSchema.Field("ms", DataType.Num))))
    def run(input: NodeInput): IO[NodeOutput] =
      runs.update(_ + 1).as(NodeOutput(Map("v" -> Json.fromString("woke")), suspendUntil = Some(input.now + input.params.hcursor.get[Long]("ms").getOrElse(0L))))
    def count: IO[Int] = runs.get
  }

  private def waits: IO[Waits] = Ref.of[IO, Int](0).map(new Waits(_))

  private def wait(id: String, ms: Long): NodeDef = NodeDef(id, "test.wait", Json.obj("ms" -> Json.fromLong(ms)), List(out(id)))

  private def cron(id: String, params: String): NodeDef = NodeDef(id, "core.cron", json(params))

  private def withTimers[A](f: (FlowEngine[IO], Waits) => IO[A]): IO[A] =
    for {
      journal <- InMemoryJournal.make[IO]
      probe <- Probe.make
      w <- waits
      result <- IOActorSystem[IO]("timers").use(system => ActorFlowEngine.make[IO](system, journal, List(probe.step, w)).flatMap(f(_, w)))
    } yield result

  private def entries(engine: FlowEngine[IO], id: String): IO[List[AuditEntry]] = engine.audit(id).map(_.get.entries)

  private def scheduled(es: List[AuditEntry]): List[NodeWaiting] = es.collect { case w: NodeWaiting if w.decision.isInstanceOf[Decision.Scheduled] => w }

  private val simple = flow("timer", List(step("a"), wait("w", 400), step("b")), List(edge("a", "w"), edge("w", "b")))

  "a node that waits" - {

    "stays running until its time, says when in the status and the audit, and then goes on" in
      withTimers { (engine, w) =>
        for {
          id <- run(engine, simple)
          waiting <- awaitUntil(engine.status(id))(_.exists(_.nodes("w").fireAt.isDefined))
          during = waiting.get
          done <- finished(engine, id)
          trail <- entries(engine, id)
          runs <- w.count
        } yield {
          status(during, "w") shouldBe NodeStatus.Running
          during.nodes("w").fireAt.get should be > 0L
          // Nothing after it started while it waited.
          status(during, "b") shouldBe NodeStatus.Pending
          done.status shouldBe Status.Succeeded
          done.state shouldBe json("""{"out":{"a":"a","w":"woke","b":"b"}}""")
          done.nodes("w").fireAt shouldBe None
          runs shouldBe 1

          val wait = scheduled(trail)
          wait should have size 1
          wait.head.node shouldBe "w"
          wait.head.decision shouldBe Decision.Scheduled(during.nodes("w").fireAt.get)
          val fin = trail.collect { case f: NodeFinished if f.node == "w" => f }.head
          position(trail, "node-started", "w") should be < position(trail, "node-waiting", "w")
          position(trail, "node-waiting", "w") should be < position(trail, "node-finished", "w")
          // The time of the wait is in the duration of the node, and it did not go on before.
          fin.at should be >= wait.head.decision.asInstanceOf[Decision.Scheduled].fireAt
          fin.durationMs should be >= 300L
          trail.map(_.seq) shouldBe trail.indices.map(_.toLong).toList
        }
      }

    "a time that is not in the future is no wait" in
      withTimers { (engine, _) =>
        for {
          id <- run(engine, flow("nowait", List(wait("w", -5), step("b")), List(edge("w", "b"))))
          done <- finished(engine, id)
          trail <- entries(engine, id)
        } yield {
          done.status shouldBe Status.Succeeded
          scheduled(trail) shouldBe empty
        }
      }

    "cannot be retried while it waits: the instance has not ended" in
      withTimers { (engine, _) =>
        for {
          id <- run(engine, flow("busy", List(wait("w", 600)), Nil))
          _ <- awaitUntil(engine.status(id))(_.exists(_.nodes("w").fireAt.isDefined))
          result <- engine.retry(id).attempt
          _ <- finished(engine, id)
        } yield result.left.map(_.getClass.getSimpleName) shouldBe Left("NotFinished")
      }

    "the wait survives a restart: it is set again from the journal and the node does not run twice" in {
      for {
        journal <- InMemoryJournal.make[IO]
        probe <- Probe.make
        w <- waits
        id <- IOActorSystem[IO]("before").use { system =>
          for {
            engine <- ActorFlowEngine.make[IO](system, journal, List(probe.step, w))
            id <- run(engine, flow("durable", List(wait("w", 1200), step("b")), List(edge("w", "b"))))
            _ <- awaitUntil(engine.status(id))(_.exists(_.nodes("w").fireAt.isDefined))
          } yield id
        }
        result <- IOActorSystem[IO]("after").use { system =>
          for {
            engine <- ActorFlowEngine.make[IO](system, journal, List(probe.step, w))
            s <- finished(engine, id)
            trail <- entries(engine, id)
          } yield (s, trail)
        }
        runs <- w.count
      } yield {
        val (s, trail) = result
        s.status shouldBe Status.Succeeded
        s.state shouldBe json("""{"out":{"w":"woke","b":"b"}}""")
        runs shouldBe 1
        scheduled(trail) should have size 1
        trail.count(e => e.kind == "node-finished" && e.nodeOf.contains("w")) shouldBe 1
      }
    }

    "a time that passed while the server was down goes on at once when it comes back" in {
      for {
        journal <- InMemoryJournal.make[IO]
        probe <- Probe.make
        w <- waits
        id <- IOActorSystem[IO]("down").use { system =>
          for {
            engine <- ActorFlowEngine.make[IO](system, journal, List(probe.step, w))
            id <- run(engine, flow("late", List(wait("w", 250), step("b")), List(edge("w", "b"))))
            _ <- awaitUntil(engine.status(id))(_.exists(_.nodes("w").fireAt.isDefined))
          } yield id
        }
        _ <- IO.sleep(500.millis)
        result <- IOActorSystem[IO]("up").use { system =>
          ActorFlowEngine.make[IO](system, journal, List(probe.step, w)).flatMap(e => finished(e, id).timeout(2.seconds))
        }
        runs <- w.count
      } yield {
        result.status shouldBe Status.Succeeded
        runs shouldBe 1
      }
    }
  }

  "the cron node" - {

    "as a timer waits for the next time of its expression" in
      withTimers { (engine, _) =>
        for {
          id <- run(engine, flow("cron-timer", List(step("a"), cron("tick", """{"expression":"* * * * * *"}"""), step("b")), List(edge("a", "tick"), edge("tick", "b"))))
          done <- finished(engine, id)
          trail <- entries(engine, id)
        } yield {
          done.status shouldBe Status.Succeeded
          val wait = scheduled(trail).head
          val fireAt = wait.decision.asInstanceOf[Decision.Scheduled].fireAt
          fireAt % 1000 shouldBe 0L
          fireAt - wait.at should (be > 0L and be <= 1000L)
          val fin = trail.collect { case f: NodeFinished if f.node == "tick" => f }.head
          fin.info("fireAt") shouldBe Json.fromLong(fireAt)
          fin.info("expression") shouldBe Json.fromString("* * * * * *")
          fin.port shouldBe "out"
        }
      }

    "at the start of a flow it does not wait" in
      withTimers { (engine, _) =>
        for {
          id <- run(engine, flow("cron-launcher", List(cron("tick", """{"expression":"@yearly"}"""), step("b")), List(edge("tick", "b"))))
          done <- finished(engine, id).timeout(2.seconds)
          trail <- entries(engine, id)
        } yield {
          done.status shouldBe Status.Succeeded
          scheduled(trail) shouldBe empty
        }
      }

    "fails through the error port with a time zone that does not exist, and when the wait is longer than maxWait" in
      withTimers { (engine, _) =>
        val head = flow("cron-bad", List(step("a"), cron("tick", """{"expression":"@yearly","timezone":"Mars/Phobos"}""")), List(edge("a", "tick")))
        val long = flow("cron-long", List(step("a"), cron("tick", """{"expression":"@yearly","maxWait":"1h"}""")), List(edge("a", "tick")))
        for {
          bad <- run(engine, head).flatMap(finished(engine, _))
          far <- run(engine, long).flatMap(finished(engine, _))
        } yield {
          bad.status shouldBe Status.Failed
          bad.nodes("tick").error.get should include("'Mars/Phobos' is not a time zone")
          far.status shouldBe Status.Failed
          far.nodes("tick").error.get should include("beyond maxWait 1h")
        }
      }

    "in a simulation the wait does not happen: it is woken at once and time moves on to when it would have gone on" in
      withTimers { (engine, _) =>
        val chained = flow(
          "sim",
          List(step("a"), cron("t1", """{"expression":"@yearly"}"""), cron("t2", """{"expression":"@yearly"}"""), step("b")),
          List(edge("a", "t1"), edge("t1", "t2"), edge("t2", "b"))
        )
        val started = System.nanoTime()
        for {
          trace <- engine.simulate(chained, Json.obj(), Map.empty)
        } yield {
          (System.nanoTime() - started) should be < 5.seconds.toNanos
          trace.status shouldBe Status.Succeeded
          trace.steps.map(_.nodeId) shouldBe List("a", "t1", "t2", "b")
          trace.steps.map(_.outcome).distinct shouldBe List("ran")
          val waits = trace.audit.collect { case w: NodeWaiting => w.decision }.collect { case Decision.Scheduled(t) => t }
          waits should have size 2
          // The second one is a year after the first: it saw the time of the first when it asked.
          (waits(1) - waits(0)) should be >= 365L * 24 * 3600 * 1000
          trace.audit.collect { case f: NodeFinished if f.node == "t2" => f.simulation.map(_.outcome) } shouldBe List(Some("ran"))
        }
      }
  }

  "InstanceMachine" - {
    "tells the timers that wait, does not run them again, and wakes one only in its attempt" in {
      IO {
        val f = flow("m", List(wait("w", 1000), step("b")), List(edge("w", "b")))
        val started = InstanceEvent.Started("m", 1, f, Json.obj(), Origin.Manual(), 1L)
        val s1 = InstanceMachine(InstanceMachine.empty("i"), started)
        val catalog = new NodeCatalog(List(NodeDescriptor("test.wait", "Test", "Wait", ConfigSchema(List(ConfigSchema.Field("ms", DataType.Num)))), NodeDescriptor("test.step", "Test", "Step", ConfigSchema(List(ConfigSchema.Field("tag", DataType.Str, required = false))))))
        val adv = InstanceMachine.advance(s1, catalog, Redactor(catalog), 10L)
        adv.launches.map(_.nodeId) shouldBe List("w")
        adv.launches.head.input.now shouldBe 10L
        adv.launches.head.input.entry shouldBe true
        val s2 = adv.state
        val answered = InstanceMachine.complete(s2, "w", 1, Right(NodeOutput(Map("v" -> Json.True), suspendUntil = Some(5000L))), catalog, 20L)
        answered.map(_.getClass.getSimpleName) shouldBe List("NodeSuspended")
        val s3 = answered.foldLeft(s2)(InstanceMachine.apply)
        InstanceMachine.suspended(s3) shouldBe List(("w", 1, 5000L))
        InstanceMachine.running(s3, 30L) shouldBe Nil
        InstanceMachine.wake(s3, "w", 2, 6000L) shouldBe Nil
        InstanceMachine.wake(s3, "other", 1, 6000L) shouldBe Nil
        val woken = InstanceMachine.wake(s3, "w", 1, 6000L)
        woken.map(_.getClass.getSimpleName) shouldBe List("NodeSucceeded")
        val s4 = woken.foldLeft(s3)(InstanceMachine.apply)
        s4.nodes("w").status shouldBe NodeStatus.Succeeded
        s4.nodes("w").suspended shouldBe None
        InstanceMachine.suspended(s4) shouldBe Nil
        // A second wake finds nothing to wake.
        InstanceMachine.wake(s4, "w", 1, 7000L) shouldBe Nil
      }
    }
  }
}
