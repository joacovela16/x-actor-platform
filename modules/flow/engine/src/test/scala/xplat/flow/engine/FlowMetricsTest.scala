package xplat.flow.engine

import cats.effect.IO
import xplat.core.io.actor.IOActorSystem

/** The metrics come from the audit of the instances, as they run. */
class FlowMetricsTest extends EngineSpec {

  private def withMetrics[A](f: (FlowEngine[IO], Probe, FlowMetrics[IO]) => IO[A]): IO[A] =
    for {
      journal <- InMemoryJournal.make[IO]
      probe <- Probe.make
      metrics <- FlowMetrics.make[IO]()
      result <- IOActorSystem[IO]("metrics").use(system =>
        ActorFlowEngine.make[IO](system, journal, List(probe.step), observer = Some(metrics)).flatMap(f(_, probe, metrics))
      )
    } yield result

  "instances and nodes are counted and timed, by flow and by kind, and a failure names its node" in
    withMetrics { (engine, probe, metrics) =>
      val ok = flow("ok", List(step("a"), step("b")), List(edge("a", "b")))
      val bad = flow("bad", List(step("x")), Nil)
      for {
        a <- run(engine, ok)
        _ <- finished(engine, a)
        b <- engine.start("ok", None, io.circe.Json.obj(), None)
        _ <- finished(engine, b)
        _ <- probe.breakDown("x")
        c <- run(engine, bad)
        _ <- finished(engine, c)
        text <- metrics.render(List(FlowMetrics.Sample("flow_extra", "Something else.", List("k" -> "v\"q"), 1)))
        health <- metrics.health
      } yield {
        text should include("""flow_instances_started_total{flow="ok",origin="manual"} 2""")
        text should include("""flow_instances_finished_total{flow="ok",status="succeeded"} 2""")
        text should include("""flow_instances_finished_total{flow="bad",status="failed"} 1""")
        text should include("""flow_node_failures_total{flow="bad",node="x",kind="test.step"} 1""")
        text should include("""flow_node_duration_seconds_count{kind="test.step"} 5""")
        text should include("""flow_node_duration_seconds_bucket{kind="test.step",le="+Inf"} 5""")
        text should include("""flow_instance_duration_seconds_count{flow="ok"} 2""")
        text should include("# TYPE flow_instance_duration_seconds histogram")
        text should include("""flow_extra{k="v\"q"} 1""")
        text should not include "flow_instances_running{"

        val byFlow = health.map(h => h.flowId -> h).toMap
        byFlow("ok").succeeded shouldBe 2
        byFlow("ok").failed shouldBe 0
        byFlow("ok").p95Ms should not be empty
        byFlow("bad").failed shouldBe 1
        byFlow("bad").lastFailures.map(f => (f.instanceId, f.error)) shouldBe List(c -> Some("x: x is broken"))
      }
    }
}
