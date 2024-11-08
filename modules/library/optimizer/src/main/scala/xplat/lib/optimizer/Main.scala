package xplat.lib.optimizer

import cats.effect.IO
import cats.free.Free
import cats.~>

import scala.language.implicitConversions

object Main {

  final case class ElseDecl[T]() {
    def Else(condBody: Expr[T]): Expr[T] = ???
    def ElseIf(cond: Expr[Boolean])(condBody: Expr[T]): ElseDecl[T] = ???
  }

  def If[T](cond: Expr[Boolean])(thenBody: Expr[T]): ElseDecl[T] = ???

  def set[T](implicit sc: sourcecode.Name): SSet[T] = SSet.Open(sc.value)
  def set[T](head: T, tail: T*)(implicit sc: sourcecode.Name): SSet[T] = SSet.Literal(sc.value, head +: tail)
  def union[T](head: SSet[T], tail: SSet[T]*)(implicit sc: sourcecode.Name): SSet[T] = SSet.Union(sc.value, head +: tail)
  def set(from: Int, to: Int)(implicit sc: sourcecode.Name): SSet[Int] = SSet.Range(sc.value, from, to)

  def param[T](implicit k: Typed[T, Kind]): Param[T, Unit] = ???
  def variable[T](implicit k: IsValidVariable[T]): Variable[T, Unit] = variable()
  def variable[T](lowerBound: Option[T] = None, upperBound: Option[T] = None)(implicit k: IsValidVariable[T]): Variable[T, Unit] = ???

  def sum[D](value: D): SumExpr[D] = ???

  def minimize[T](expr: Expr[T])(implicit c: IsValidVariable[T]): Model = ???
  def maximize[T](expr: Expr[T])(implicit c: IsValidVariable[T]): Free[AMPLCommand, Unit] = ???

  def constraint(expr: Expr[Boolean]): Free[AMPLCommand, Unit] = ???
  def constraint[D](value: D): SubjectTo[D] = ???
//  def st(expr: Expr[Boolean]): Constraint = subjectTo(expr)
//  def baseOn[D](value: D): SubjectTo[D] = ???

  final case class SubjectTo[D]() {
//    def apply[A, H](a:H)(implicit ops: ConditionDomain.Aux[D, Expr, Boolean, A, H]):Free[AMPLCommand, Unit] = ???
  }

  object SubjectTo {

    implicit class Predef[D, Apply, Handler](subjectTo: SubjectTo[D])(implicit ops: ConditionDomain.Aux[D, Expr, Boolean, Apply, Handler]) {
      def apply(x: Handler): Free[AMPLCommand, Unit] = ???
    }

  }

  implicit def var2free[T, D](variable: Variable[T, D]): Free[AMPLCommand, Variable[T, D]] = ???
  implicit def par2free[T, D](param: Param[T, D]): Free[AMPLCommand, Param[T, D]] = ???
  implicit def set2free[T](set: SSet[T]): Free[AMPLCommand, SSet[T]] = ???

  def solve: Free[AMPLCommand, Unit] = ???

  val interpreter: AMPLCommand ~> IO = new (AMPLCommand ~> IO) {
    override def apply[A](fa: AMPLCommand[A]): IO[A] = ???
  }

  def model(m: Free[AMPLCommand, Unit]) = {
    m.foldMap(interpreter)
  }

  model {
    for {
      s1 <- set[Int]
      p1 <- param[Int].domain(s1).project(x => x * 2)

      x <- variable[Double]
      y <- variable[Double]

      _ <- maximize(x * 3 + y * 4)

      _ <- constraint(x + y * 2 <= 14)

      _ <- solve
    } yield ()
  }

  model {
    for {
      p <- set[String]

      a <- param[Int].domain(p)
      b <- param[Int]
      c <- param[Int].domain(p)
      u <- param[Int].domain(p)

      x <- variable[Double].domain(p)

      _ <- maximize(sum(p)(j => c(j) * x(j)))

      _ <- constraint(sum(p)(j => (1 / a(j)) + x(j)) <= b)
      _ <- constraint(p)(j => x(j) <= u(j))
      _ <- constraint(p)(j => x(j) >= 0)

      _ <- solve
    } yield ()
  }

}
