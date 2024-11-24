package xplat.lib.optimizer

import cats.effect.{IO, IOApp}
import xplat.lib.optimizer.model.dsl._
import xplat.lib.optimizer.solver.{Shared, Simplifier}

import scala.language.implicitConversions

object MainIO extends IOApp.Simple with OptimizerDSL {

  val m1 = {
//    val s1 = set[Int]
    // val p1 = param[Int].domain(s1).project(x => x * 2)

    val x = variable[Double]
    val y = variable[Double]

    maximize(x * 3 + y * 4)
      .subjectTo(
        x + y * 2 <= 14
      )
  }

  val m2 = {
//    val s1 = set[Int]
//    val p1 = param[Int].domain(s1).project(x => x * 2)

    val x = variable[Double]
    val y = variable[Double]

    maximize(x * 3 + y * 4) subjectTo (
      x + y * 2 <= 14
    )
  }

  val m3 = {
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

    val x1 = variable[Int]
    val x2 = variable[Int]
    val x3 = variable[Int]
    val x4 = variable[Int]

  /*  pprint.pprintln(
      Shared.simplify(x1 + x2 + x2) == x1 + (2 * x2)
    )

    pprint.pprintln(
      Shared.simplify(x1 + (2 * (x2 + x3))) == x1 + (2 * x2 + 2 * x3)
    )

    pprint.pprintln(
      Shared.simplify(x3 + x2 + x1) == x1 + (x2 + x3)
    )

    pprint.pprintln(
      Shared.simplify(x2 + x3 + x1) == x1 + (x2 + x3)
    )

    pprint.pprintln(
      Shared.simplify(2 * x1 - x1) == Expr.Numerical(x1)
    )

    pprint.pprintln(
      Shared.simplify(x2 + x1 + x3 + x4) == (((x1 + x2) + x3) + x4)
    )
*/
    pprint.pprintln(
      Simplifier.simplify(x1 + (2*x2) + x1 )
    )


    Optimizer[IO](model)(
      context
    )

  }

  /*val m4 = {
    val S1 = set[String]
    val S2 = set[String]
    val P1 = param[Int].domain(S1, S2)
    val P2 = param[Int]
    Context().put(P1)(
      ("1", "1") -> 1,
      ("1", "2") -> 2,
      ("2", "1") -> 3,
      ("2", "2") -> 4
    )
    Context().put(P2, 3)
  }*/

  override def run: IO[Unit] = m3
  /*
      def main(args: Array[String]): Unit = {

        pprint.pprintln(m3)

      }*/

}
