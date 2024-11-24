package xplat.lib.optimizer

import org.scalatest.funsuite.AnyFunSuite
import xplat.lib.optimizer.model.dsl.Var

class UnapplyTest extends AnyFunSuite with OptimizerDSL {

  val x1: Var[Int, Unit] = variable[Int]
  val x2: Var[Int, Unit] = variable[Int]
  val x3: Var[Int, Unit] = variable[Int]
  val x4: Var[Int, Unit] = variable[Int]

  test("01"){

  }
}
