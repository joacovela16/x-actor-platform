package xplat.lib.optimizer

import org.scalatest.funsuite.AnyFunSuite
import xplat.lib.optimizer.model.dsl.Context
import xplat.lib.optimizer.solver.Expander

class ExpanderTest extends AnyFunSuite with OptimizerDSL {

  val P = set[String]
  val a = param[Int].domain(P)
  val b = param[Int]
  val c = param[Int].domain(P)
  val u = param[Int].domain(P)
  val x = variable[Double].domain(P)

  val model = maximize(sum(P)(j => c(j) * x(j))).subjectTo(
    sum(P)(j => (1.0 / a(j)) + x(j)) <= b,
    forall(P)(j => x(j) <= u(j)),
    forall(P)(j => x(j) >= 0)
  )

  val context = Context()
    .put(P, "bands", "coils")
    .put(a, "bands" -> 200, "coils" -> 140)
    .put(c, "bands" -> 25, "coils" -> 30)
    .put(u, "bands" -> 6000, "coils" -> 4000)
    .put(b, 40)

  test("01") {
    pprint.pprintln(
      Expander(model, context)
    )
  }

}
