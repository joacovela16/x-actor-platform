package xplat.lib.optimizer

import scala.language.implicitConversions

object Main {

  def If(cond: Expr.Logical)(thenBody: Expr.Numerical): ElseDecl = ElseDecl(cond, Nil, thenBody)

  def set[T](implicit sc: sourcecode.Name, v: Typed[T, ValidType]): SSet[T] = SSet.Open(sc.value)
  def set[T](head: T, tail: T*)(implicit sc: sourcecode.Name): SSet[T] = SSet.Literal(sc.value, head +: tail)
  def union[T](head: SSet[T], tail: SSet[T]*)(implicit sc: sourcecode.Name): SSet[T] = SSet.Union(sc.value, head +: tail)
  def set(from: Int, to: Int)(implicit sc: sourcecode.Name): SSet[Int] = SSet.Range(sc.value, from, to)

  def param[T](implicit k: Typed[T, NumberType], sc: sourcecode.Name): Param[T, Unit] = Param(sc.value, k.value)
  def variable[T](implicit k: IsValidVariable[T], t: Typed[T, NumberType], sc: sourcecode.Name): Var[T, Unit] = variable()

  def variable[T](lowerBound: Option[T] = None, upperBound: Option[T] = None)(implicit k: IsValidVariable[T], t: Typed[T, NumberType], sc: sourcecode.Name): Var[T, Unit] = {
    Var(sc.value, t.value, lowerBound.map(k.asDouble), upperBound.map(k.asDouble))
  }

  def sum[D](value: D): SumBuilder[D] = SumBuilder(value)

  def minimize(expr: Expr.Numerical): Model = Model(ObjectiveKind.Min, expr, Nil)
  def maximize(expr: Expr.Numerical): Model = Model(ObjectiveKind.Max, expr, Nil)

  

  def forall[D](value: D): SubjectTo[D] = SubjectTo(value)

  val m1 = {
    val s1 = set[Int]
    val p1 = param[Int].domain(s1).project(x => x * 2)

    val x = variable[Double]
    val y = variable[Double]

    maximize(x * 3 + y * 4)
      .subjectTo(
        x + y * 2 <= 14
      )
  }

  val m2 = {
    val s1 = set[Int]
    val p1 = param[Int].domain(s1).project(x => x * 2)

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

    maximize {
      sum(P)(j => c(j) * x(j))
    } subjectTo (
      sum(P)(j => (1.0 / a(j)) + x(j)) <= b,
      forall(P)(j => x(j) <= u(j)),
      forall(P)(j => x(j) >= 0)
    )

  }

  def main(args: Array[String]): Unit = {
    pprint.pprintln(m1)
  }

}
