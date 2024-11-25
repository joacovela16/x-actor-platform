package xplat.lib.optimizer

import xplat.lib.optimizer.model.dsl._

import scala.language.implicitConversions

trait OptimizerDSL {

  implicit class DoublePredef(n: Double) {
    def *[T](v: Var[T, Unit]): Expr.Numerical = Expr.Numerical.Binary(n, Arithmentics.Times, v)
    def +[T](v: Var[T, Unit]): Expr.Numerical = Expr.Numerical.Binary(n, Arithmentics.Plus, v)
  }

  implicit def mk[T](v: Var[T, Unit]): Expr.Numerical = Expr.Numerical.GetVar(Nil, v.asDecl)
  implicit def mk[T](v: Param[T, Unit]): Expr.Numerical = Expr.Numerical.GetParam(Nil, v.asDecl)

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

}

object OptimizerDSL extends OptimizerDSL {}
