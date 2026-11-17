package xplat.flow.engine

import cats.effect.IO
import cats.implicits._
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine.InstanceState.{NodeStatus, Status}
import xplat.flow.model._

import scala.concurrent.duration._

class FlowEngineTest extends EngineSpec {

  // ---- running ----

  "an instance" - {

    "runs a linear flow and writes what each node declares in the state" in withEngine { (engine, _) =>
      val f = flow("linear", List(step("a"), step("b"), step("c")), List(edge("a", "b"), edge("b", "c")))
      for {
        id <- run(engine, f)
        events <- engine.events(id).compile.toList
        s <- engine.status(id).map(_.get)
      } yield {
        s.status shouldBe Status.Succeeded
        s.state shouldBe json("""{"out":{"a":"a","b":"b","c":"c"}}""")
        events.head shouldBe a[AuditEntry.InstanceStarted]
        events.last should matchPattern { case AuditEntry.InstanceFinished(_, _, "succeeded", _) => }
        s.nodes.values.map(_.status).toSet shouldBe Set(NodeStatus.Succeeded)
      }
    }

    "fills the state from the inputs, and rejects inputs that do not fit" in withEngine { (engine, _) =>
      val f = flow("inputs", List(step("a", "read" -> Json.fromString("name"))), Nil, inputs = List(OutputDecl("name", path("name"), DataType.Str)))
      for {
        saved <- save(engine, f)
        id <- engine.start("inputs", None, json("""{"name":"x"}"""), None)
        s <- finished(engine, id)
        missing <- engine.start("inputs", None, json("{}"), None).attempt
        wrong <- engine.start("inputs", None, json("""{"name":1}"""), None).attempt
        unknown <- engine.start("nothing", None, json("{}"), None).attempt
      } yield {
        saved.revision shouldBe 1
        valueAt(s, "name") shouldBe Some(Json.fromString("x"))
        missing.left.toOption.get shouldBe a[FlowEngine.InvalidInputs]
        wrong.left.toOption.get shouldBe a[FlowEngine.InvalidInputs]
        unknown shouldBe Left(FlowEngine.FlowNotFound("nothing", None))
      }
    }

    "does not start twice with an instance id that exists" in withEngine { (engine, probe) =>
      for {
        _ <- save(engine, flow("once", List(step("a")), Nil))
        first <- engine.start("once", None, Json.obj(), Some("fixed"))
        _ <- finished(engine, first)
        second <- engine.start("once", None, Json.obj(), Some("fixed"))
        n <- probe.count("a")
      } yield {
        (first, second) shouldBe (("fixed", "fixed"))
        n shouldBe 1
      }
    }

    "broadcasts through a port and the join `all` waits for every branch" in withEngine { (engine, probe) =>
      val f = flow(
        "all",
        List(step("a"), step("b", "delayMs" -> Json.fromInt(60)), step("c", "delayMs" -> Json.fromInt(120)), step("d")),
        List(edge("a", "b"), edge("a", "c"), edge("b", "d"), edge("c", "d"))
      )
      for {
        id <- run(engine, f)
        events <- engine.events(id).compile.toList
        s <- engine.status(id).map(_.get)
        d <- probe.count("d")
      } yield {
        s.status shouldBe Status.Succeeded
        d shouldBe 1
        val startOfD = position(events, "node-started", "d")
        position(events, "node-finished", "b") should be < startOfD
        position(events, "node-finished", "c") should be < startOfD
      }
    }

    "the join `any` runs with the first branch and only once" in withEngine { (engine, probe) =>
      val any = step("d").copy(joinPolicy = JoinPolicy.AnyInput)
      val f = flow(
        "any",
        List(step("a"), step("b"), step("c", "delayMs" -> Json.fromInt(150)), any),
        List(edge("a", "b"), edge("a", "c"), edge("b", "d"), edge("c", "d"))
      )
      for {
        id <- run(engine, f)
        events <- engine.events(id).compile.toList
        s <- engine.status(id).map(_.get)
        d <- probe.count("d")
      } yield {
        d shouldBe 1
        s.status shouldBe Status.Succeeded
        position(events, "node-started", "d") should be < position(events, "node-finished", "c")
      }
    }

    "a branch that was not taken is dead, and the join does not wait for it" in withEngine { (engine, probe) =>
      val f = flow(
        "branch",
        List(NodeDef("check", IfNodeKind, json("""{"path":"flag"}""")), step("yes"), step("no"), step("join")),
        List(edge("check", "yes", "then"), edge("check", "no", "else"), edge("yes", "join"), edge("no", "join")),
        inputs = List(OutputDecl("flag", path("flag"), DataType.Bool))
      )
      for {
        _ <- save(engine, f)
        t <- engine.start("branch", None, json("""{"flag":true}"""), None).flatMap(finished(engine, _))
        e <- engine.start("branch", None, json("""{"flag":false}"""), None).flatMap(finished(engine, _))
        runs <- probe.runs
      } yield {
        (t.status, status(t, "yes"), status(t, "no"), status(t, "join")) shouldBe ((Status.Succeeded, NodeStatus.Succeeded, NodeStatus.Dead, NodeStatus.Succeeded))
        (e.status, status(e, "yes"), status(e, "no"), status(e, "join")) shouldBe ((Status.Succeeded, NodeStatus.Dead, NodeStatus.Succeeded, NodeStatus.Succeeded))
        runs shouldBe Map("yes" -> 1, "no" -> 1, "join" -> 2)
      }
    }

    "an If that cannot decide fails through its error port" in withEngine { (engine, _) =>
      val f = flow(
        "iferr",
        List(NodeDef("check", IfNodeKind, json("""{"path":"flag","source":"memory"}""")), step("handler")),
        List(edge("check", "handler", Port.Error))
      )
      for {
        id <- run(engine, f)
        s <- finished(engine, id)
      } yield {
        status(s, "check") shouldBe NodeStatus.Failed
        s.nodes("check").error.get should include("not in the memory")
        status(s, "handler") shouldBe NodeStatus.Succeeded
        s.status shouldBe Status.Succeeded
      }
    }

    "a failure goes through the error port; one that nothing takes fails the instance" in withEngine { (engine, probe) =>
      val handled = flow("handled", List(step("x"), step("next"), step("handler")), List(edge("x", "next"), edge("x", "handler", Port.Error)))
      val unhandled = flow("unhandled", List(step("x"), step("next")), List(edge("x", "next")))
      for {
        _ <- probe.breakDown("x")
        a <- run(engine, handled).flatMap(finished(engine, _))
        b <- run(engine, unhandled).flatMap(finished(engine, _))
      } yield {
        (a.status, status(a, "x"), status(a, "next"), status(a, "handler")) shouldBe ((Status.Succeeded, NodeStatus.Failed, NodeStatus.Dead, NodeStatus.Succeeded))
        a.nodes("x").error shouldBe Some("x is broken")
        (b.status, status(b, "next")) shouldBe ((Status.Failed, NodeStatus.Dead))
      }
    }

    "an answer that does not fit what the node declared makes it fail" in withEngine { (engine, _) =>
      val typed = step("a").copy(outputs = List(OutputDecl("v", path("n"), DataType.Num)))
      for {
        id <- run(engine, flow("typed", List(typed), Nil))
        s <- finished(engine, id)
      } yield {
        status(s, "a") shouldBe NodeStatus.Failed
        s.nodes("a").error.get should include("must be num")
        s.state shouldBe Json.obj()
      }
    }

    "a node that takes too long fails" in {
      for {
        journal <- InMemoryJournal.make[IO]
        probe <- Probe.make
        s <- IOActorSystem[IO]("slow").use { system =>
          for {
            engine <- ActorFlowEngine.make[IO](system, journal, List(probe.step), ActorFlowEngine.Config(nodeTimeout = 100.millis))
            id <- run(engine, flow("slow", List(step("a", "delayMs" -> Json.fromInt(2000))), Nil))
            s <- finished(engine, id)
          } yield s
        }
      } yield {
        status(s, "a") shouldBe NodeStatus.Failed
        s.nodes("a").error.get should include("did not finish")
      }
    }
  }

  // ---- retry ----

  "retrying an instance" - {

    "does not run again the nodes that succeeded and are set to skip" in withEngine { (engine, probe) =>
      val once = step("a").copy(runPolicy = RunPolicy.OnceSucceeded)
      val f = flow("retry", List(once, step("b"), step("c")), List(edge("a", "b"), edge("a", "c")))
      for {
        _ <- probe.breakDown("b")
        id <- run(engine, f)
        first <- finished(engine, id)
        _ <- probe.fix("b")
        _ <- engine.retry(id)
        second <- finished(engine, id)
        runs <- probe.runs
      } yield {
        first.status shouldBe Status.Failed
        second.status shouldBe Status.Succeeded
        second.state shouldBe json("""{"out":{"a":"a","b":"b","c":"c"}}""")
        runs shouldBe Map("a" -> 1, "b" -> 2, "c" -> 2)
      }
    }

    "a node set to skip when it failed is not run, and keeps failing" in withEngine { (engine, probe) =>
      val f = flow("skipfail", List(step("a").copy(runPolicy = RunPolicy(whenFailed = RunPolicy.Skip))), Nil)
      for {
        _ <- probe.breakDown("a")
        id <- run(engine, f)
        _ <- finished(engine, id)
        _ <- probe.fix("a")
        _ <- engine.retry(id)
        s <- finished(engine, id)
        n <- probe.count("a")
      } yield {
        n shouldBe 1
        (status(s, "a"), s.status) shouldBe ((NodeStatus.Failed, Status.Failed))
      }
    }

    "runs again what changed: the configuration of a node, or what it reads" in withEngine { (engine, probe) =>
      def version(value: String) = flow(
        "changing",
        List(step("a", "value" -> Json.fromString(value)).copy(runPolicy = RunPolicy.OnceSucceeded), step("b")),
        List(edge("a", "b"))
      )
      for {
        _ <- probe.breakDown("b")
        id <- run(engine, version("one"))
        _ <- finished(engine, id)
        _ <- probe.fix("b")
        // Nothing changed in `a`: it is reused. The new revision is not used unless it is asked for.
        _ <- save(engine, version("two"))
        _ <- engine.retry(id)
        reused <- finished(engine, id)
        runsAfterReuse <- probe.count("a")
        _ <- engine.retry(id, useLatest = true)
        changed <- finished(engine, id)
        runsAfterChange <- probe.count("a")
      } yield {
        runsAfterReuse shouldBe 1
        valueAt(reused, "out.a") shouldBe Some(Json.fromString("one"))
        runsAfterChange shouldBe 2
        valueAt(changed, "out.a") shouldBe Some(Json.fromString("two"))
        changed.revision shouldBe 2
      }
    }

    "a node that reads a value that changed is run again" in withEngine { (engine, probe) =>
      def version(value: String) = flow(
        "reading",
        List(step("a", "value" -> Json.fromString(value)), step("b", "read" -> Json.fromString("out.a")).copy(runPolicy = RunPolicy.OnceSucceeded), step("c")),
        List(edge("a", "b"), edge("b", "c"))
      )
      for {
        _ <- probe.breakDown("c")
        id <- run(engine, version("one"))
        _ <- finished(engine, id)
        _ <- probe.fix("c")
        _ <- save(engine, version("two"))
        _ <- engine.retry(id, useLatest = true)
        _ <- finished(engine, id)
        b <- probe.count("b")
      } yield b shouldBe 2
    }

    "is only possible once the instance ended" in withEngine { (engine, _) =>
      for {
        id <- run(engine, flow("busy", List(step("a", "delayMs" -> Json.fromInt(300))), Nil))
        early <- engine.retry(id).attempt
        _ <- finished(engine, id)
        missing <- engine.retry("nope").attempt
      } yield {
        early shouldBe Left(FlowEngine.NotFinished(id))
        missing shouldBe Left(FlowEngine.InstanceNotFound("nope"))
      }
    }
  }

  // ---- recovery ----

  "an instance" - {

    "is recovered from the journal by another engine" in {
      for {
        journal <- InMemoryJournal.make[IO]
        probe <- Probe.make
        before <- IOActorSystem[IO]("first").use { system =>
          for {
            engine <- ActorFlowEngine.make[IO](system, journal, List(probe.step))
            id <- run(engine, flow("recovered", List(step("a"), step("b")), List(edge("a", "b"))))
            s <- finished(engine, id)
            trail <- engine.audit(id)
          } yield (id, s, trail)
        }
        after <- IOActorSystem[IO]("second").use { system =>
          ActorFlowEngine.make[IO](system, journal, List(probe.step)).flatMap(e => (e.status(before._1), e.audit(before._1)).tupled)
        }
      } yield {
        after._1 shouldBe Some(before._2)
        // The audit trail made from the journal is the one that was made live.
        after._2 shouldBe before._3
        after._2.get.entries.map(_.seq) shouldBe after._2.get.entries.indices.map(_.toLong).toList
      }
    }

    "goes on after a restart: the nodes that were running run again" in {
      for {
        journal <- InMemoryJournal.make[IO]
        probe <- Probe.make
        id <- IOActorSystem[IO]("crashing").use { system =>
          for {
            engine <- ActorFlowEngine.make[IO](system, journal, List(probe.step))
            id <- run(engine, flow("resumed", List(step("a", "delayMs" -> Json.fromInt(400)), step("b")), List(edge("a", "b"))))
            _ <- awaitUntil(engine.status(id))(_.exists(s => status(s, "a") == NodeStatus.Running))
          } yield id
        }
        result <- IOActorSystem[IO]("restarted").use { system =>
          for {
            engine <- ActorFlowEngine.make[IO](system, journal, List(probe.step))
            s <- finished(engine, id)
          } yield s
        }
        runs <- probe.count("a")
      } yield {
        result.status shouldBe Status.Succeeded
        result.state shouldBe json("""{"out":{"a":"a","b":"b"}}""")
        runs shouldBe 2
      }
    }
  }

  // ---- subflows ----

  "a node that runs another flow" - {

    val child = flow(
      "child",
      List(step("c", "value" -> Json.fromInt(42))),
      Nil,
      inputs = List(OutputDecl("n", path("n"), DataType.Num)),
      outputs = List(OutputDecl("r", path("out.c"), DataType.Num))
    )

    def parent(params: String, taken: List[OutputDecl]) = flow(
      "parent",
      List(NodeDef("call", "core.subflow", json(params), taken)),
      Nil,
      inputs = List(OutputDecl("x", path("x"), DataType.Num))
    )

    "waits for it and takes its outputs" in withEngine { (engine, probe) =>
      for {
        _ <- save(engine, child)
        id <- run(engine, parent("""{"flowId":"child","inputs":{"n":"x"}}""", List(OutputDecl("r", path("got"), DataType.Num))), json("""{"x":1}"""))
        s <- finished(engine, id)
        n <- probe.count("c")
      } yield {
        s.status shouldBe Status.Succeeded
        valueAt(s, "got") shouldBe Some(Json.fromInt(42))
        n shouldBe 1
      }
    }

    "goes on without waiting" in withEngine { (engine, probe) =>
      for {
        _ <- save(engine, child)
        id <- run(engine, parent("""{"flowId":"child","wait":false,"inputs":{"n":"x"}}""", Nil), json("""{"x":1}"""))
        s <- finished(engine, id)
        _ <- awaitUntil(probe.count("c"))(_ == 1)
      } yield s.status shouldBe Status.Succeeded
    }

    "fails when the flow it runs fails" in withEngine { (engine, probe) =>
      for {
        _ <- probe.breakDown("c")
        _ <- save(engine, child)
        id <- run(engine, parent("""{"flowId":"child","inputs":{"n":"x"}}""", List(OutputDecl("r", path("got"), DataType.Num))), json("""{"x":1}"""))
        s <- finished(engine, id)
      } yield {
        s.status shouldBe Status.Failed
        s.nodes("call").error.get should include("failed")
      }
    }
  }

  // ---- simulation ----

  "a simulation" - {

    val f = flow(
      "sim",
      List(
        NodeDef("check", IfNodeKind, json("""{"path":"flag"}""")),
        NodeDef("send", "test.write", json("""{"tag":"send"}"""), List(out("send"))),
        step("other")
      ),
      List(edge("check", "send", "then"), edge("check", "other", "else")),
      inputs = List(OutputDecl("flag", path("flag"), DataType.Bool))
    )

    "runs what has no effects, and does not run what writes outside" in withEngine { (engine, probe) =>
      for {
        t <- engine.simulate(f, json("""{"flag":true}"""), Map.empty)
        runs <- probe.runs
      } yield {
        t.status shouldBe Status.Succeeded
        t.steps.map(s => (s.nodeId, s.outcome, s.port)) shouldBe List(("check", "ran", Some("then")), ("other", "skipped", None), ("send", "simulated", Some("out")))
        // The write answers with a default for the type it declared.
        t.state shouldBe json("""{"flag":true,"out":{"send":null}}""")
        runs shouldBe empty
      }
    }

    "answers with the mocks" in withEngine { (engine, probe) =>
      for {
        mocked <- engine.simulate(f, json("""{"flag":true}"""), Map("send" -> Mock.Output(Map("v" -> Json.fromString("fake")))))
        failed <- engine.simulate(f, json("""{"flag":true}"""), Map("send" -> Mock.Fail("no network")))
        other <- engine.simulate(f, json("""{"flag":false}"""), Map("other" -> Mock.Output(Map("v" -> Json.fromInt(1)))))
        runs <- probe.runs
      } yield {
        mocked.steps.find(_.nodeId == "send").map(_.outcome) shouldBe Some("mocked")
        valueAt(mocked.state, "out.send") shouldBe Some(Json.fromString("fake"))
        failed.status shouldBe Status.Failed
        failed.steps.find(_.nodeId == "send").flatMap(_.error) shouldBe Some("no network")
        other.steps.map(s => (s.nodeId, s.outcome)) shouldBe List(("check", "ran"), ("send", "skipped"), ("other", "mocked"))
        runs shouldBe empty
      }
    }

    "a mock that does not fit makes the node fail" in withEngine { (engine, _) =>
      engine.simulate(f, json("""{"flag":true}"""), Map("send" -> Mock.Output(Map("nope" -> Json.True)))).map { t =>
        t.steps.find(_.nodeId == "send").flatMap(_.error).get should include("did not declare")
      }
    }

    "rejects inputs that do not fit, and does not need the flow to be saved" in withEngine { (engine, _) =>
      engine.simulate(f, json("{}"), Map.empty).attempt.map(_.left.toOption.get shouldBe a[FlowEngine.InvalidInputs])
    }

    "simulates the flow that a node calls" in withEngine { (engine, probe) =>
      val child = flow("child", List(step("c", "value" -> Json.fromInt(7))), Nil, inputs = List(OutputDecl("n", path("n"), DataType.Num)), outputs = List(OutputDecl("r", path("out.c"), DataType.Num)))
      val parent = flow(
        "parent",
        List(NodeDef("call", "core.subflow", json("""{"flowId":"child","inputs":{"n":"x"}}"""), List(OutputDecl("r", path("got"), DataType.Num)))),
        Nil,
        inputs = List(OutputDecl("x", path("x"), DataType.Num))
      )
      for {
        _ <- save(engine, child)
        t <- engine.simulate(parent, json("""{"x":1}"""), Map.empty)
      } yield {
        t.steps.map(s => (s.nodeId, s.outcome)) shouldBe List(("call", "simulated"))
        valueAt(t.state, "got") shouldBe Some(Json.fromInt(7))
      }
    }

    def valueAt(state: Json, p: String): Option[Json] = path(p).get(state)
  }

  // ---- store ----

  "the store" - {

    "keeps every revision and validates before saving" in withEngine { (engine, _) =>
      val f = flow("doc", List(step("a")), Nil)
      for {
        one <- save(engine, f)
        two <- save(engine, f.copy(name = "renamed"))
        invalid <- engine.store.save(flow("bad", List(NodeDef("x", "no.such.kind")), Nil))
        latest <- engine.store.get("doc", None)
        first <- engine.store.get("doc", Some(1))
        list <- engine.store.list
        bad <- engine.store.get("bad", None)
        _ <- engine.store.delete("doc")
        gone <- engine.store.get("doc", None)
      } yield {
        (one.revision, two.revision) shouldBe ((1, 2))
        invalid.left.toOption.get.map(_.code) shouldBe List("unknown-kind")
        latest.map(_.name) shouldBe Some("renamed")
        first.map(_.name) shouldBe Some("doc")
        list shouldBe List(FlowSummary("doc", "renamed", 2))
        bad shouldBe None
        gone shouldBe None
      }
    }
  }

}
