package xplat.flow.engine

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.circe.Json
import xplat.flow.engine.CronScheduler.Schedule
import xplat.flow.model._

import java.time.Instant
import scala.concurrent.duration._

/** The scheduler is told what time it is (`pollAt`), so what it does is the same every time it is tested. */
class CronSchedulerTest extends EngineSpec {

  private def cron(text: String) = CronExpression.parse(text).fold(e => throw new IllegalArgumentException(e), identity)
  private def at(text: String): Instant = Instant.parse(text)

  private val target = flow(
    "launched",
    List(NodeDef("check", IfNodeKind, json("""{"path":"approved"}"""))),
    Nil,
    inputs = List(OutputDecl("approved", path("approved"), DataType.Bool))
  )

  private def schedule(expression: String, misfire: String = "fire-once", timezone: String = "UTC", flowId: String = "launched", key: String = "launched:tick", inputs: String = """{"approved":true}""") =
    Schedule(key, flowId, cron(expression), timezone, misfire, json(inputs), Origin.Cron("launched/tick"))

  private final class Fixture(val engine: FlowEngine[IO], val current: Ref[IO, List[Schedule]], val checkpoints: Checkpoints[IO], val poller: CronScheduler.Poller[IO]) {
    def started(origin: String = "cron:"): IO[List[InstanceSummary]] = engine.search(InstanceQuery(origin = Some(origin), limit = 1000)).map(_.items)
    def ids: IO[Set[String]] = started().map(_.map(_.id).toSet)
    def set(s: Schedule*): IO[Unit] = current.set(s.toList)
  }

  private def withScheduler[A](initial: List[Schedule], settings: CronScheduler.Settings = CronScheduler.Settings())(f: Fixture => IO[A]): IO[A] =
    withEngine { (engine, _) =>
      for {
        _ <- save(engine, target)
        current <- Ref.of[IO, List[Schedule]](initial)
        checkpoints <- Checkpoints.inMemory[IO]
        poller <- CronScheduler.poller[IO](engine, current.get, checkpoints, settings)
        result <- f(new Fixture(engine, current, checkpoints, poller))
      } yield result
    }

  private def tick(text: String): Long = at(text).toEpochMilli

  "a schedule that is seen for the first time counts from now: it does not run the past" in
    withScheduler(List(schedule("* * * * * *"))) { fx =>
      for {
        _ <- fx.poller.pollAt(at("2024-01-01T10:00:00Z"))
        started <- fx.ids
        saved <- fx.checkpoints.get("launched:tick")
      } yield {
        started shouldBe empty
        saved shouldBe Some(tick("2024-01-01T10:00:00Z"))
      }
    }

  "every time that comes starts an instance of the flow with its inputs, named after the schedule and the time" in
    withScheduler(List(schedule("0 * * * * *"))) { fx =>
      for {
        _ <- fx.poller.pollAt(at("2024-01-01T10:00:00Z"))
        _ <- fx.poller.pollAt(at("2024-01-01T10:00:30Z"))
        _ <- fx.poller.pollAt(at("2024-01-01T10:01:00Z"))
        _ <- fx.poller.pollAt(at("2024-01-01T10:02:01Z"))
        started <- fx.started()
        first <- finished(fx.engine, s"launched:tick:${tick("2024-01-01T10:01:00Z")}")
        saved <- fx.checkpoints.get("launched:tick")
      } yield {
        started.map(_.id).toSet shouldBe Set(s"launched:tick:${tick("2024-01-01T10:01:00Z")}", s"launched:tick:${tick("2024-01-01T10:02:00Z")}")
        started.map(_.origin.label).distinct shouldBe List("cron:launched/tick")
        first.state shouldBe json("""{"approved":true}""")
        first.nodes("check").port shouldBe Some("then")
        saved shouldBe Some(tick("2024-01-01T10:02:00Z"))
      }
    }

  "polling again for a time that was dealt with starts nothing, even if the checkpoint was lost" in
    withScheduler(List(schedule("0 * * * * *"))) { fx =>
      for {
        _ <- fx.poller.pollAt(at("2024-01-01T10:00:00Z"))
        _ <- fx.poller.pollAt(at("2024-01-01T10:01:00Z"))
        before <- fx.ids
        _ <- fx.poller.pollAt(at("2024-01-01T10:01:00Z"))
        again <- fx.ids
        // A restart that lost what it had saved: the same time is dealt with again, and the engine knows the instance already.
        _ <- fx.checkpoints.set("launched:tick", tick("2024-01-01T10:00:00Z"))
        _ <- fx.poller.pollAt(at("2024-01-01T10:01:00Z"))
        afterLoss <- fx.ids
      } yield {
        before should have size 1
        again shouldBe before
        afterLoss shouldBe before
      }
    }

  "times missed while the server was down: fire-once starts one instance for them, skip starts none" in
    withScheduler(Nil) { fx =>
      val hourly = "0 0 * * * *"
      for {
        _ <- fx.set(schedule(hourly, "fire-once", key = "once"), schedule(hourly, "skip", key = "skipped"))
        _ <- fx.poller.pollAt(at("2024-01-01T00:30:00Z"))
        // The server was down for five hours: 01:00, 02:00, 03:00, 04:00 and 05:00 passed.
        _ <- fx.poller.pollAt(at("2024-01-01T05:30:00Z"))
        afterDown <- fx.ids
        // A time that is only a few seconds late is not a miss: both run it.
        _ <- fx.poller.pollAt(at("2024-01-01T06:00:02Z"))
        afterOnTime <- fx.ids
      } yield {
        afterDown shouldBe Set(s"once:${tick("2024-01-01T05:00:00Z")}")
        afterOnTime shouldBe Set(s"once:${tick("2024-01-01T05:00:00Z")}", s"once:${tick("2024-01-01T06:00:00Z")}", s"skipped:${tick("2024-01-01T06:00:00Z")}")
      }
    }

  "a schedule that goes away is forgotten: enabling it later does not make up for the time it was off" in
    withScheduler(List(schedule("0 0 * * * *"))) { fx =>
      for {
        _ <- fx.poller.pollAt(at("2024-01-01T00:30:00Z"))
        _ <- fx.set()
        _ <- fx.poller.pollAt(at("2024-01-01T01:30:00Z"))
        forgotten <- fx.checkpoints.get("launched:tick")
        _ <- fx.set(schedule("0 0 * * * *"))
        _ <- fx.poller.pollAt(at("2024-01-01T05:30:00Z"))
        afterBack <- fx.ids
        _ <- fx.poller.pollAt(at("2024-01-01T06:00:01Z"))
        later <- fx.ids
      } yield {
        forgotten shouldBe None
        afterBack shouldBe empty
        later shouldBe Set(s"launched:tick:${tick("2024-01-01T06:00:00Z")}")
      }
    }

  "time zones: the schedule runs at local times" in
    withScheduler(List(schedule("0 0 9 * * *", timezone = "Europe/Madrid"))) { fx =>
      for {
        _ <- fx.poller.pollAt(at("2024-07-01T00:00:00Z"))
        _ <- fx.poller.pollAt(at("2024-07-01T07:00:01Z"))
        ids <- fx.ids
      } yield ids shouldBe Set(s"launched:tick:${tick("2024-07-01T07:00:00Z")}")
    }

  "what cannot be started is logged and given up, not tried over and over, and does not stop the other schedules" in
    withScheduler(Nil) { fx =>
      for {
        _ <- fx.set(
          schedule("0 * * * * *", key = "missing", flowId = "nowhere"),
          schedule("0 * * * * *", key = "badinputs", inputs = """{"approved":"yes"}"""),
          schedule("0 * * * * *", key = "badzone", timezone = "Mars/Phobos"),
          schedule("0 * * * * *", key = "fine")
        )
        _ <- fx.poller.pollAt(at("2024-01-01T10:00:00Z"))
        _ <- fx.poller.pollAt(at("2024-01-01T10:01:00Z"))
        ids <- fx.ids
        missing <- fx.checkpoints.get("missing")
        zone <- fx.checkpoints.get("badzone")
      } yield {
        ids shouldBe Set(s"fine:${tick("2024-01-01T10:01:00Z")}")
        // The time of the flow that does not exist was given up, so the next poll does not try it again.
        missing shouldBe Some(tick("2024-01-01T10:01:00Z"))
        zone shouldBe None
      }
    }

  "with a clock of its own it runs in the background and starts what comes" in
    withScheduler(Nil) { fx =>
      CronScheduler.resource[IO](fx.engine, fx.current.get, fx.checkpoints, CronScheduler.Settings(pollEvery = 50.millis)).use { _ =>
        for {
          _ <- fx.set(schedule("* * * * * *"))
          ids <- awaitUntil(fx.ids)(_.size >= 2).timeout(10.seconds)
        } yield {
          ids.foreach(_ should startWith("launched:tick:"))
          ids.map(_.split(":").last.toLong).foreach(_ % 1000 shouldBe 0L)
        }
      }
    }

  "the schedules of the flows" - {

    val launcher = flow(
      "f",
      List(
        NodeDef("tick", "core.cron", json("""{"expression":"*/5 * * * *","timezone":"Europe/Madrid","misfire":"skip","inputs":{"approved":false}}""")),
        NodeDef("timer", "core.cron", json("""{"expression":"@hourly"}""")),
        NodeDef("broken", "core.cron", json("""{"expression":"x"}""")),
        step("a")
      ),
      List(edge("a", "timer"))
    )

    "are the cron nodes where the execution starts" in IO {
      val schedules = FlowSchedules.of(launcher)
      // The timer has an incoming edge, and the one with a bad expression is left out.
      schedules.map(_.key) shouldBe List("f:tick")
      val s = schedules.head
      s.flowId shouldBe "f"
      s.timezone shouldBe "Europe/Madrid"
      s.misfire shouldBe "skip"
      s.inputs shouldBe json("""{"approved":false}""")
      s.origin shouldBe Origin.Cron("f/tick")
      s.origin.label shouldBe "cron:f/tick"
      s.expression.text shouldBe "*/5 * * * *"
    }

    "only count while the flow is enabled" in
      withEngine { (engine, _) =>
        for {
          deployments <- Deployments.inMemory[IO]
          _ <- engine.store.save(launcher.copy(nodes = launcher.nodes.filterNot(_.id == "broken"), edges = List(edge("a", "timer"))))
          none <- FlowSchedules.enabled(engine.store, deployments)
          _ <- deployments.set("f", enabled = true)
          on <- FlowSchedules.enabled(engine.store, deployments)
          isOn <- deployments.isEnabled("f")
          _ <- deployments.set("f", enabled = false)
          off <- FlowSchedules.enabled(engine.store, deployments)
          list <- deployments.enabled
        } yield {
          none shouldBe empty
          on.map(_.key) shouldBe List("f:tick")
          isOn shouldBe true
          off shouldBe empty
          list shouldBe empty
        }
      }
  }

  "the origin of a schedule is part of the audit and can be searched" in
    withScheduler(List(schedule("0 * * * * *"))) { fx =>
      for {
        _ <- fx.poller.pollAt(at("2024-01-01T10:00:00Z"))
        _ <- fx.poller.pollAt(at("2024-01-01T10:01:00Z"))
        id = s"launched:tick:${tick("2024-01-01T10:01:00Z")}"
        _ <- finished(fx.engine, id)
        trail <- fx.engine.audit(id)
        byLabel <- fx.engine.search(InstanceQuery(origin = Some("cron:launched/tick")))
      } yield {
        trail.get.entries.collect { case e: AuditEntry.InstanceStarted => e.origin }.head shouldBe Origin.Cron("launched/tick")
        io.circe.Encoder[AuditEntry].apply(trail.get.entries.head).hcursor.get[String]("origin") shouldBe Right("cron:launched/tick")
        byLabel.items.map(_.id) shouldBe List(id)
      }
    }
}
