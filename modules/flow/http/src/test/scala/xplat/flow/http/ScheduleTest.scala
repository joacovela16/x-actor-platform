package xplat.flow.http

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import org.http4s.Status
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.engine.{CronScheduler, Origin}
import xplat.flow.model._

import java.nio.file.{Files, Path}

/** Switching flows with a cron launcher on and off, the schedules of triggers, and keeping both in the data directory. */
class ScheduleTest extends HttpSpec {

  private val IfNodeKind = "core.if"

  private val launcher = FlowDef(
    "nightly",
    "Nightly",
    nodes = List(
      NodeDef("tick", "core.cron", json("""{"expression":"0 3 * * *","timezone":"Europe/Madrid","misfire":"skip","inputs":{"approved":true}}""")),
      NodeDef("check", IfNodeKind, json("""{"path":"approved"}"""))
    ),
    edges = List(Edge("tick", "check")),
    inputs = List(OutputDecl("approved", path("approved"), DataType.Bool))
  )

  "enabling a flow" - {

    "is off until it is asked for, and says its schedules with the next time each runs" in withApp { app =>
      for {
        saved <- app.save(launcher)
        before <- app.get("/api/flows/nightly/schedule")
        listBefore <- app.get("/api/flows")
        on <- app.post("/api/flows/nightly/enable", Json.Null)
        listOn <- app.get("/api/flows")
        off <- app.post("/api/flows/nightly/disable", Json.Null)
      } yield {
        saved.status shouldBe Status.Ok
        before.status shouldBe Status.Ok
        before.json.hcursor.get[Boolean]("enabled") shouldBe Right(false)
        val s = before.json.hcursor.downField("schedules").downArray
        s.get[String]("node") shouldBe Right("tick")
        s.get[String]("expression") shouldBe Right("0 3 * * *")
        s.get[String]("timezone") shouldBe Right("Europe/Madrid")
        s.get[String]("misfire") shouldBe Right("skip")
        s.get[Long]("nextFireAt").toOption.get should be > System.currentTimeMillis()
        listBefore.json.asArray.get.map(_.hcursor.get[Boolean]("enabled")) shouldBe Vector(Right(false))
        on.status shouldBe Status.Ok
        on.json.hcursor.get[Boolean]("enabled") shouldBe Right(true)
        listOn.json.asArray.get.map(_.hcursor.get[Boolean]("enabled")) shouldBe Vector(Right(true))
        off.json.hcursor.get[Boolean]("enabled") shouldBe Right(false)
      }
    }

    "a flow that does not exist is a 404, and deleting a flow switches it off" in withApp { app =>
      for {
        missing <- app.post("/api/flows/ghost/enable", Json.Null)
        missingSchedule <- app.get("/api/flows/ghost/schedule")
        _ <- app.save(launcher)
        _ <- app.post("/api/flows/nightly/enable", Json.Null)
        deleted <- app.call(org.http4s.Method.DELETE, "/api/flows/nightly")
        on <- app.deployments.enabled
      } yield {
        missing.status shouldBe Status.NotFound
        missingSchedule.status shouldBe Status.NotFound
        deleted.status shouldBe Status.NoContent
        on shouldBe empty
      }
    }

    "a flow without cron launchers has no schedules, but can still be enabled" in withApp { app =>
      for {
        _ <- app.save(FlowDef("plain", "Plain", List(NodeDef("a", IfNodeKind, json("""{"path":"x"}"""))), inputs = List(OutputDecl("x", path("x"), DataType.Bool))))
        r <- app.post("/api/flows/plain/enable", Json.Null)
      } yield r.json.hcursor.downField("schedules").as[List[Json]] shouldBe Right(Nil)
    }
  }

  "the audit and status show a timer that waits" in withApp { app =>
    val timer = FlowDef(
      "timer", "Timer",
      List(NodeDef("a", IfNodeKind, json("""{"path":"x"}""")), NodeDef("wait", "core.cron", json("""{"expression":"* * * * * *"}"""))),
      List(Edge("a", "wait", "then")),
      inputs = List(OutputDecl("x", path("x"), DataType.Bool))
    )
    for {
      _ <- app.save(timer)
      started <- app.post("/api/flows/timer/instances", json("""{"inputs":{"x":true}}"""))
      id = started.json.hcursor.get[String]("instanceId").toOption.get
      done <- app.finished(id)
      audit <- app.get(s"/api/instances/$id/audit")
    } yield {
      done.hcursor.get[String]("status") shouldBe Right("succeeded")
      val waiting = audit.json.hcursor.downField("entries").as[List[Json]].toOption.get.filter(_.hcursor.get[String]("kind").contains("node-waiting"))
      waiting should have size 1
      waiting.head.hcursor.downField("decision").get[String]("reason") shouldBe Right("scheduled")
      waiting.head.hcursor.downField("decision").get[Long]("fireAt").toOption.get should be > 0L
    }
  }

  "cron triggers" - {

    "are checked when they are saved" in withApp { app =>
      def put(body: String) = app.put("/api/triggers/t", json(body))
      for {
        _ <- app.save(launcher)
        ok <- put("""{"id":"t","flowId":"nightly","kind":"cron","cron":"*/5 * * * *","timezone":"Europe/Madrid"}""")
        noExpression <- put("""{"id":"t","flowId":"nightly","kind":"cron"}""")
        badExpression <- put("""{"id":"t","flowId":"nightly","kind":"cron","cron":"61 * * * *"}""")
        badZone <- put("""{"id":"t","flowId":"nightly","kind":"cron","cron":"@daily","timezone":"Mars/Phobos"}""")
      } yield {
        ok.status shouldBe Status.Ok
        ok.json.hcursor.get[String]("timezone") shouldBe Right("Europe/Madrid")
        noExpression.status shouldBe Status.UnprocessableContent
        badExpression.status shouldBe Status.UnprocessableContent
        badExpression.text should include("minute: 61 is out of range")
        badZone.status shouldBe Status.UnprocessableContent
        badZone.text should include("not a time zone")
      }
    }

    "make a schedule for each one, started with the inputs they carry" in IO {
      val triggers = List(
        TriggerDef("c1", "nightly", TriggerDef.Kind.Cron, cron = Some("0 9 * * *"), timezone = Some("Europe/Madrid"), inputs = Map("approved" -> Json.True)),
        TriggerDef("c2", "other", TriggerDef.Kind.Cron, cron = Some("@hourly")),
        TriggerDef("h", "nightly", TriggerDef.Kind.Http, path = Some("x")),
        TriggerDef("bad", "nightly", TriggerDef.Kind.Cron, cron = Some("x"))
      )
      val schedules = TriggerSchedules.of(triggers)
      schedules.map(_.key) shouldBe List("trigger:c1", "trigger:c2")
      schedules.head.flowId shouldBe "nightly"
      schedules.head.timezone shouldBe "Europe/Madrid"
      schedules.head.inputs shouldBe json("""{"approved":true}""")
      schedules.head.origin shouldBe Origin.Trigger("c1", None)
      schedules(1).timezone shouldBe "UTC"
      schedules(1).misfire shouldBe "fire-once"
    }
  }
}

class ScheduleStoresTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def withDir[A](f: Path => IO[A]): IO[A] = IO.blocking(Files.createTempDirectory("flow-schedule")).flatMap(f)

  "what is enabled is kept in the data directory" in withDir { dir =>
    val file = dir.resolve("deployments.json")
    for {
      first <- JsonDeployments.make[IO](Some(file))
      _ <- first.set("a", enabled = true)
      _ <- first.set("b", enabled = true)
      _ <- first.set("a", enabled = false)
      again <- JsonDeployments.make[IO](Some(file))
      on <- again.enabled
      isB <- again.isEnabled("b")
      isA <- again.isEnabled("a")
      memory <- JsonDeployments.make[IO](None)
      _ <- memory.set("x", enabled = true)
      inMemory <- memory.enabled
    } yield {
      on shouldBe Set("b")
      isB shouldBe true
      isA shouldBe false
      inMemory shouldBe Set("x")
    }
  }

  "the checkpoints of the schedules are kept in the data directory" in withDir { dir =>
    val file = dir.resolve("checkpoints.json")
    for {
      first <- JsonCheckpoints.make[IO](Some(file))
      _ <- first.set("a:tick", 1000L)
      _ <- first.set("b:tick", 2000L)
      _ <- first.set("a:tick", 3000L)
      _ <- first.delete("b:tick")
      again <- JsonCheckpoints.make[IO](Some(file))
      a <- again.get("a:tick")
      b <- again.get("b:tick")
    } yield {
      a shouldBe Some(3000L)
      b shouldBe None
    }
  }
}
