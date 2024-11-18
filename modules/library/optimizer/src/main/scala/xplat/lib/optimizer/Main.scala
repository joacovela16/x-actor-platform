package xplat.lib.optimizer

import cats.effect.{IO, IOApp}
import xplat.core.io.actor.IOActorSystemBuilder.ioActorSystem
import xplat.lib.optimizer.model._

import scala.language.implicitConversions

object Main extends IOApp.Simple {
  implicit def mk[T](v: Var[T, Unit]): Expr.Numerical = Expr.Numerical.GetVar(Nil, v.asDecl)
//  implicit def mk[T](v: Arg[T]): Expr.Numerical = ???

  def If(cond: Expr.Logical)(thenBody: Expr.Numerical): ElseDecl = ElseDecl(cond, Nil, thenBody)

  def set[T](implicit sc: sourcecode.Name, v: Typed[T, DomainType]): SSet[T] = SSet.Open(sc.value)
  def set[T](head: T, tail: T*)(implicit sc: sourcecode.Name, c: Datum.Codec[T]): SSet[T] = SSet.Literal(sc.value, head +: tail, c)
  def set(from: Int, to: Int)(implicit sc: sourcecode.Name): SSet[Int] = SSet.Range(sc.value, from, to)

  def param[T](implicit k: Typed[T, NumberType], sc: sourcecode.Name): Param[T, Unit] = Param(sc.value, k.value)
  def variable[T](implicit k: IsNumber[T], t: Typed[T, NumberType], sc: sourcecode.Name): Var[T, Unit] = variable()

  def variable[T](lowerBound: Option[T] = None, upperBound: Option[T] = None)(implicit k: IsNumber[T], t: Typed[T, NumberType], sc: sourcecode.Name): Var[T, Unit] = {
    Var(sc.value, t.value, lowerBound.map(k.asDouble), upperBound.map(k.asDouble))
  }

  def sum[D](value: D): SumBuilder[D] = SumBuilder(value)

  def minimize(expr: Expr.Numerical): Model = Model(ObjectiveKind.Min, expr, Nil)
  def maximize(expr: Expr.Numerical): Model = Model(ObjectiveKind.Max, expr, Nil)

  def forall[D](value: D): SubjectTo[D] = SubjectTo(value)

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

    Optimizer[IO](model)(
      Context().put(P, "A", "B")
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
