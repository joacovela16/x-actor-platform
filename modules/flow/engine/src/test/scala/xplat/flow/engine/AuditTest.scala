package xplat.flow.engine

import cats.effect.IO
import io.circe.Json
import io.circe.syntax._
import xplat.flow.engine.AuditEntry._
import xplat.flow.engine.InstanceState.Status
import xplat.flow.model._

class AuditTest extends EngineSpec {

  // check (If on `flag`) -then-> yes -> join <- no <-else- check ; join -> fin
  private val audited = flow(
    "audited",
    List(
      NodeDef("check", IfNodeKind, json("""{"path":"flag"}""")),
      step("yes").copy(runPolicy = RunPolicy.OnceSucceeded),
      step("no"),
      step("join"),
      step("fin")
    ),
    List(edge("check", "yes", "then"), edge("check", "no", "else"), edge("yes", "join"), edge("no", "join"), edge("join", "fin")),
    inputs = List(OutputDecl("flag", path("flag"), DataType.Bool))
  )

  private def finishedOf(entries: List[AuditEntry], node: String): List[NodeFinished] = entries.collect { case n: NodeFinished if n.node == node => n }
  private def skippedOf(entries: List[AuditEntry], node: String): List[NodeSkipped] = entries.collect { case n: NodeSkipped if n.node == node => n }

  "the audit trail of an instance" - {

    "explains why each node ran or did not: an If, a dead branch, a join, a failure, a reused result" in withEngine { (engine, probe) =>
      for {
        _ <- probe.breakDown("fin")
        _ <- save(engine, audited)
        id <- engine.start("audited", None, json("""{"flag":true}"""), None, Origin.Trigger("orders", Some("req-1")))
        _ <- finished(engine, id)
        first <- engine.audit(id).map(_.get)
        _ <- probe.fix("fin")
        _ <- engine.retry(id, by = Some("alice"))
        _ <- finished(engine, id)
        second <- engine.audit(id).map(_.get)
        live <- engine.events(id).compile.toList
      } yield {
        val before = first.entries
        val all = second.entries

        // The instance: who started it, with what, which definition, how it ended.
        first.instance.origin shouldBe Origin.Trigger("orders", Some("req-1"))
        first.instance.flowId shouldBe "audited"
        first.instance.revision shouldBe 1
        first.instance.inputs shouldBe json("""{"flag":true}""")
        first.instance.status shouldBe Status.Failed
        before.head should matchPattern { case InstanceStarted(0, _, _, "audited", 1, _, Origin.Trigger("orders", Some("req-1")), _) => }
        before.last should matchPattern { case InstanceFinished(_, _, "failed", _) => }

        // The If decided `then` because of the value it read, and that is what killed the other branch.
        val check = finishedOf(before, "check").head
        (check.status, check.port) shouldBe (("succeeded", "then"))
        before.collect { case n: AuditEntry.NodeStarted if n.node == "check" => n.reads }.head shouldBe List(Read("flag", Some(Json.True)))
        before should contain(EdgeFired(before.collectFirst { case e: EdgeFired => e.seq }.get, check.at, EdgeRef("check", "then", "yes")))
        before.collect { case e: EdgeDead => (e.edge, e.reason) } should contain((EdgeRef("check", "else", "no"), "the node left through 'then'"))
        skippedOf(before, "no").map(_.decision) shouldBe List(
          Decision.DeadBranch(List(Cause("check", "else", "other-port", Some("then"), List(Read("flag", Some(Json.True))))))
        )

        // The join said what it was waiting for, and ran when it had everything.
        before.collect { case j: NodeWaiting => (j.node, j.decision) } shouldBe List(("join", Decision.WaitingJoin(List(EdgeRef("yes", "out", "join")))))
        position(before, "node-finished", "join") should be > position(before, "node-finished", "yes")

        // What was written, and what it replaced.
        finishedOf(before, "yes").head.outputs shouldBe List(Written("v", "out.yes", None, Json.fromString("yes")))
        before.collect { case c: StateChanged => (c.node, c.changes) } should contain(("yes", List(Written("v", "out.yes", None, Json.fromString("yes")))))

        // The failure, with its message, and the error port that nothing took.
        val fin = finishedOf(before, "fin").head
        (fin.status, fin.error, fin.port) shouldBe (("failed", Some("fin is broken"), Port.Error))

        // After the retry: it is asked for by someone, and a node that succeeded is not run again.
        val retryAt = all.indexWhere(_.isInstanceOf[RetryRequested])
        all(retryAt) should matchPattern { case RetryRequested(_, _, Origin.Retry(`id`, Some("alice")), 1) => }
        val after = all.drop(retryAt)
        skippedOf(after, "yes").map(_.decision) shouldBe List(Decision.RunPolicy("whenSucceeded=skip", Port.Out, List(Written("v", "out.yes", None, Json.fromString("yes"))), None))
        finishedOf(after, "check").size shouldBe 1 // the default policy runs it again
        finishedOf(after, "fin").map(_.status) shouldBe List("succeeded")
        all.last should matchPattern { case InstanceFinished(_, _, "succeeded", _) => }
        second.instance.status shouldBe Status.Succeeded
        second.instance.endedAt should not be empty

        // It is one trail, in order, and what is followed live is the same.
        all.map(_.seq) shouldBe all.indices.map(_.toLong).toList
        all.map(_.at) shouldBe all.map(_.at).sorted
        live shouldBe all
      }
    }

    "links a node that runs a flow to the instance that ran it" in withEngine { (engine, _) =>
      val child = flow("child", List(step("c", "value" -> Json.fromInt(1))), Nil, outputs = List(OutputDecl("r", path("out.c"), DataType.Num)))
      val parent = flow(
        "parent",
        List(NodeDef("call", "core.subflow", json("""{"flowId":"child"}"""), List(OutputDecl("r", path("got"), DataType.Num)))),
        Nil
      )
      for {
        _ <- save(engine, child)
        id <- run(engine, parent)
        _ <- finished(engine, id)
        trail <- engine.audit(id).map(_.get)
        call = finishedOf(trail.entries, "call").head
        childTrail <- engine.audit(call.childInstanceId.get).map(_.get)
      } yield {
        call.status shouldBe "succeeded"
        childTrail.instance.flowId shouldBe "child"
        childTrail.instance.origin shouldBe Origin.Subflow(id, "call")
      }
    }

    "hides secrets: in the configuration of a node, in the inputs and in the paths that are marked" in
      withEngineConfig(ActorFlowEngine.Config(secretPaths = List(path("creds.token")))) { (engine, _) =>
        val f = flow(
          "secrets",
          List(
            NodeDef("use", "test.secret", json("""{"tag":"use","apiKey":"sk-123"}"""), List(out("use"))),
            step("show", "read" -> Json.fromString("creds.token"))
          ),
          List(edge("use", "show")),
          inputs = List(OutputDecl("token", path("creds.token"), DataType.Str))
        )
        for {
          id <- run(engine, f, json("""{"token":"hunter2"}"""))
          s <- finished(engine, id)
          trail <- engine.audit(id).map(_.get)
          live <- engine.events(id).compile.toList
        } yield {
          val started = trail.entries.collect { case n: AuditEntry.NodeStarted => n.node -> n }.toMap
          started("use").params.hcursor.downField("apiKey").focus shouldBe Some(Redactor.Hidden)
          started("use").params.hcursor.get[String]("tag") shouldBe Right("use")
          started("show").reads shouldBe List(Read("creds.token", Some(Redactor.Hidden)))
          trail.instance.inputs shouldBe Json.obj("token" -> Redactor.Hidden)
          trail.entries.collectFirst { case i: InstanceStarted => i.inputs }.get shouldBe Json.obj("token" -> Redactor.Hidden)
          // Nothing of it anywhere in what is shown, live or afterwards, though the instance does have the value.
          (trail.asJson.noSpaces + live.asJson.noSpaces) should not include "hunter2"
          (trail.asJson.noSpaces + live.asJson.noSpaces) should not include "sk-123"
          valueAt(s, "creds.token") shouldBe Some(Json.fromString("hunter2"))
        }
      }

    "is written in the JSON the editor reads" in withEngine { (engine, probe) =>
      for {
        _ <- probe.breakDown("fin")
        _ <- save(engine, audited)
        id <- engine.start("audited", None, json("""{"flag":true}"""), None, Origin.Trigger("orders", Some("req-1")))
        _ <- finished(engine, id)
        trail <- engine.audit(id).map(_.get)
        summaries <- engine.search(InstanceQuery())
      } yield {
        val entries = trail.entries.map(_.asJson)
        def of(kind: String) = entries.filter(_.hcursor.get[String]("kind") == Right(kind))
        // The origin is the short label, with the detail beside it.
        trail.asJson.hcursor.downField("instance").get[String]("origin") shouldBe Right("trigger:orders")
        trail.asJson.hcursor.downField("instance").downField("originDetail").get[String]("requestId") shouldBe Right("req-1")
        summaries.items.asJson.hcursor.downArray.get[String]("origin") shouldBe Right("trigger:orders")
        // What the engine decided is in `decision.reason`.
        of("node-skipped").head.hcursor.downField("decision").get[String]("reason") shouldBe Right("skipped-dead-branch")
        of("node-skipped").head.hcursor.downField("decision").downField("causes").downArray.downField("decidedWith").downArray.get[String]("path") shouldBe Right("flag")
        of("node-waiting").head.hcursor.downField("decision").get[String]("reason") shouldBe Right("waiting-join")
        of("node-waiting").head.hcursor.downField("decision").downField("pending").downArray.get[String]("from") shouldBe Right("yes")
        of("edge-fired").head.hcursor.downField("edge").get[String]("to") shouldBe Right("yes")
        of("state-changed").nonEmpty shouldBe true
        entries.map(_.hcursor.get[String]("kind").toOption.get).toSet should contain allOf ("instance-started", "node-started", "node-finished", "node-skipped", "node-waiting", "edge-fired", "edge-dead", "state-changed", "instance-finished")
        // The audit entries decode again.
        trail.entries.map(_.asJson.as[AuditEntry]) shouldBe trail.entries.map(Right(_))
      }
    }

    "has an entry for what is written in the state, and what it replaced" in withEngine { (engine, _) =>
      val f = flow(
        "overwrite",
        List(step("a", "value" -> Json.fromInt(1)), step("b", "value" -> Json.fromInt(2)).copy(outputs = List(OutputDecl("v", path("out.a"), DataType.AnyJson)))),
        List(edge("a", "b"))
      )
      for {
        id <- run(engine, f)
        _ <- finished(engine, id)
        trail <- engine.audit(id).map(_.get)
      } yield finishedOf(trail.entries, "b").head.outputs shouldBe List(Written("v", "out.a", Some(Json.fromInt(1)), Json.fromInt(2)))
    }
  }

  "searching instances" - {

    "finds them by flow, status, origin and time, a page at a time" in withEngine { (engine, probe) =>
      val ok = flow("ok", List(step("a")), Nil)
      val bad = flow("bad", List(step("x")), Nil)
      for {
        _ <- probe.breakDown("x")
        _ <- save(engine, ok)
        _ <- save(engine, bad)
        a <- engine.start("ok", None, Json.obj(), Some("a"), Origin.Manual(Some("alice")))
        _ <- finished(engine, a)
        b <- engine.start("ok", None, Json.obj(), Some("b"), Origin.Trigger("orders", None))
        _ <- finished(engine, b)
        c <- engine.start("bad", None, Json.obj(), Some("c"), Origin.Trigger("billing", None))
        _ <- finished(engine, c)
        all <- engine.search(InstanceQuery())
        byFlow <- engine.search(InstanceQuery(flowId = Some("ok")))
        failed <- engine.search(InstanceQuery(status = Some(Status.Failed)))
        byKind <- engine.search(InstanceQuery(origin = Some("trigger")))
        exact <- engine.search(InstanceQuery(origin = Some("trigger:billing")))
        manual <- engine.search(InstanceQuery(origin = Some("manual")))
        prefix <- engine.search(InstanceQuery(origin = Some("trigger:bi")))
        future <- engine.search(InstanceQuery(from = Some(Long.MaxValue - 1)))
        past <- engine.search(InstanceQuery(to = Some(1L)))
        first <- engine.search(InstanceQuery(limit = 2))
        second <- engine.search(InstanceQuery(limit = 2, after = first.items.lastOption.map(_.id)))
      } yield {
        all.items.map(_.id).toSet shouldBe Set("a", "b", "c")
        byFlow.items.map(_.id).toSet shouldBe Set("a", "b")
        failed.items.map(_.id) shouldBe List("c")
        byKind.items.map(_.id).toSet shouldBe Set("b", "c")
        exact.items.map(_.id) shouldBe List("c")
        manual.items.map(_.id) shouldBe List("a")
        prefix.items.map(_.id) shouldBe List("c")
        future.items shouldBe empty
        past.items shouldBe empty
        all.items.foreach(i => i.endedAt should not be empty)
        all.items.map(_.startedAt) shouldBe all.items.map(_.startedAt).sorted.reverse
        (first.items.size, first.next) shouldBe ((2, first.items.lastOption.map(_.id)))
        (second.items.size, second.next) shouldBe ((1, None))
        (first.items ++ second.items).map(_.id).toSet shouldBe Set("a", "b", "c")
      }
    }
  }

  "a simulation" - {

    "leaves the same trail, marking what was simulated and the mock that answered" in withEngine { (engine, _) =>
      val f = flow(
        "sim",
        List(NodeDef("check", IfNodeKind, json("""{"path":"flag"}""")), NodeDef("send", "test.write", json("""{"tag":"send"}"""), List(out("send"))), step("other")),
        List(edge("check", "send", "then"), edge("check", "other", "else")),
        inputs = List(OutputDecl("flag", path("flag"), DataType.Bool))
      )
      val mock = Mock.Output(Map("v" -> Json.fromString("fake")))
      for {
        mocked <- engine.simulate(f, json("""{"flag":true}"""), Map("send" -> mock))
        defaulted <- engine.simulate(f, json("""{"flag":true}"""), Map.empty)
      } yield {
        finishedOf(mocked.audit, "check").head.simulation shouldBe Some(Simulated("ran", None))
        finishedOf(mocked.audit, "send").head.simulation shouldBe Some(Simulated("mocked", Some(mock)))
        finishedOf(defaulted.audit, "send").head.simulation shouldBe Some(Simulated("simulated", None))
        skippedOf(mocked.audit, "other").map(_.decision) shouldBe List(
          Decision.DeadBranch(List(Cause("check", "else", "other-port", Some("then"), List(Read("flag", Some(Json.True))))))
        )
        mocked.audit.head should matchPattern { case InstanceStarted(0, _, "simulation", "sim", _, _, Origin.Manual(Some("simulation")), _) => }
        mocked.audit.last should matchPattern { case InstanceFinished(_, _, "succeeded", _) => }
        mocked.audit.map(_.seq) shouldBe mocked.audit.indices.map(_.toLong).toList
        // In the JSON every entry says it is simulated, and the steps say which were mocked.
        mocked.asJson.hcursor.downField("audit").values.get.forall(_.hcursor.get[Boolean]("simulated") == Right(true)) shouldBe true
        mocked.asJson.hcursor.downField("steps").values.get.map(s => (s.hcursor.get[String]("nodeId").toOption.get, s.hcursor.get[Boolean]("mocked").toOption.get)).toList shouldBe
          List(("check", false), ("other", false), ("send", true))
      }
    }
  }

}
