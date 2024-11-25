package xplat.lib.optimizer

import org.scalatest.funsuite.AnyFunSuite
import xplat.lib.optimizer.model.dsl.Expr
import xplat.lib.optimizer.solver.Simplifier

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
    assert(Simplifier(x1 + p1 * x2 + x1) == 2 * x1 + p1 * x2)
  }

  test("02") {
    assert(Simplifier(2 * x4 + x3 + 3 * x2 + x1 + x2) == x1 + 4 * x2 + x3 + 2 * x4)
  }

  test("03") {
    assert(Simplifier(x3 + x1 + 2 * x2 + x1) == 2 * x1 + 2 * x2 + x3)
  }

  test("04") {
    assert(Simplifier(4 * x1 - x1) == 3 * x1)
  }

  test("05") {
    assert(Simplifier(4 * x1 - x1 + x1) == 4 * x1)
  }

  test("06") {
    assert(Simplifier(x1 + x2 + x2) == x1 + 2 * x2)
  }

  test("07") {
    assert(Simplifier(5 + x1) == x1 + 5)
  }

  test("08") {
    assert(Simplifier(p1 + x1) == x1 + p1)
  }

  test("09") {
    assert(Simplifier(p1 * x4 + x3 + p2 * x2 + x1 + x2) == x1 + (p2 + 1) * x2 + x3 + p1 * x4)
  }

  test("10") {
    assert(Simplifier(p1 * x4 + x3 + 2 * p2 * x2 + x1 + x2) == x1 + (2 * p2 + 1) * x2 + x3 + p1 * x4)
  }

  test("11") {
    assert(Simplifier(p1 * (x1 + x2)) == p1 * x1 + p1 * x2)
  }

  test("12") {
    assert(Simplifier(p1 * p2 * x4 + p1 * (x3 + x4) + 2 * p2 * x2) == 2 * p2 * x2 + p1 * x3 + (p1 * p2 + p1) * x4)
  }

  test("13") {
    assert(Simplifier(p1 * p2 * x4 + (p1 + p2) * (x3 + x4) + 2 * p2 * x2) == 2 * p2 * x2 + (p1 + p2) * x3 + ((p1 * p2) + (p1 + p2)) * x4)
  }

  test("14") {
    assert(Simplifier(p1 * x1 + p2 * x1 + p3 * x1) == (p1 + (p2 + p3)) * x1)
  }

  test("15") {
    assert(Simplifier((5 + x1) - x1) == Expr.Numerical(5))
  }

  test("16") {
    assert(Simplifier(p1 * (x1 + x2 + x3)) == p1 * x1 + p1 * x2 + p1 * x3)
  }

  test("17") {
    assert(Simplifier(p1/2 * (x1 + x2 + x3)) == p1/2 * x1 + p1/2 * x2 + p1/2 * x3)
  }

}
