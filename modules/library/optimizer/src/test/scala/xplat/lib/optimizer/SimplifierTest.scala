package xplat.lib.optimizer

import org.scalatest.funsuite.AnyFunSuite
import xplat.lib.optimizer.solver.{Arranger, Simplifier}

class SimplifierTest extends AnyFunSuite with OptimizerDSL {
  val x1 = variable[Int]
  val x2 = variable[Int]
  val x3 = variable[Int]
  val x4 = variable[Int]

  test("01") {
    assert(Simplifier.simplify(x1 + 2 * x2 + x1) == 2 * x1 + 2 * x2)
  }

  test("02") {
    pprint.pprintln(
      Simplifier.simplify(
        2 * x4 + x3 + 3 * x2 + x1 + x2
      )
    )
//    assert(Simplifier.simplify(x3 + x1 + 2 * x2 + x1) == 2 * x1 + 2 * x2 + x3)
  }

  test("03") {
    pprint.pprintln(
      Simplifier.simplify(x3 + x1 + 2 * x2 + x1)
    )
    assert(Simplifier.simplify(x3 + x1 + 2 * x2 + x1) == 2 * x1 + 2 * x2 + x3)
  }

}
