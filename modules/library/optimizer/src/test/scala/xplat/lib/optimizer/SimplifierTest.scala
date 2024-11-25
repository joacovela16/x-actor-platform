package xplat.lib.optimizer

import org.scalatest.funsuite.AnyFunSuite
import xplat.lib.optimizer.solver.{Arranger, Simplifier}

class SimplifierTest extends AnyFunSuite with OptimizerDSL {
  val x1 = variable[Int]
  val x2 = variable[Int]
  val x3 = variable[Int]
  val x4 = variable[Int]

  val p1 = param[Int]
  val p2 = param[Int]
  val p3 = param[Int]
  val p4 = param[Int]

  test("01") {
//    pprint.pprintln(
//      Simplifier.simplify(x1 + p1 * x2 + x1)
//    )
    assert(Simplifier.simplify(x1 + p1 * x2 + x1) == 2 * x1 + p1 * x2)
  }

  test("02") {
    assert(Simplifier.simplify(2 * x4 + x3 + 3 * x2 + x1 + x2) == x1 + 4 * x2 + x3 + 2 * x4)
  }

  test("03") {
    assert(Simplifier.simplify(x3 + x1 + 2 * x2 + x1) == 2 * x1 + 2 * x2 + x3)
  }

  test("04") {
    assert(Simplifier.simplify(4 * x1 - x1) == 3 * x1)
  }

  test("05") {
    assert(Simplifier.simplify(4 * x1 - x1 + x1) == 4 * x1)
  }

  test("06") {
    assert(Simplifier.simplify(x1 + x2 + x2) == x1 + 2 * x2)
  }

  test("07") {
    assert(Simplifier.simplify(5 + x1) == x1 + 5)
  }

  test("08") {
    assert(Simplifier.simplify(p1 + x1) == x1 + p1)
  }

}
