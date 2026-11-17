package xplat.flow.http

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.Json
import xplat.flow.engine.FlowEngine
import xplat.flow.model.{FlowDef, FlowTest}

/** Runs the tests that a flow keeps (see [[FlowTest]]), each in a simulation of its own: nothing outside is written. */
object FlowTests {

  /** The result of each test of `flow`, in order. A test whose mocks cannot be read fails with that, without running. */
  def run[F[_]: Concurrent](engine: FlowEngine[F], flow: FlowDef): F[List[FlowTest.Result]] =
    flow.tests.traverse { t =>
      t.mocks.toList.traverse { case (node, m) => ApiJson.mock(m).leftMap(e => s"mock of '$node': $e").map(node -> _) } match {
        case Left(problem) => FlowTest.Result(t.name, passed = false, List(problem), "not run", Json.Null).pure[F]
        case Right(mocks) =>
          engine
            .simulate(flow, t.inputs, mocks.toMap)
            .map { trace =>
              val ran = trace.steps.filter(_.outcome != "skipped").map(_.nodeId).toSet
              FlowTest.check(t, trace.status.name, ran, trace.state)
            }
            .handleError(e => FlowTest.Result(t.name, passed = false, List(s"the simulation failed: ${Option(e.getMessage).getOrElse(e.toString)}"), "not run", Json.Null))
      }
    }

  /** What the API answers: whether all passed, and each result. */
  def answer(results: List[FlowTest.Result]): Json =
    Json.obj("passed" -> Json.fromBoolean(results.forall(_.passed)), "results" -> io.circe.Encoder[List[FlowTest.Result]].apply(results))
}
