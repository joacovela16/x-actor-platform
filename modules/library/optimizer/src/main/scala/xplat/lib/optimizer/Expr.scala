package xplat.lib.optimizer

import scala.language.implicitConversions

sealed trait Expr[T] extends ExprOps[T]

object Expr extends ExprLP {
  final case class If[T](cond: Expr[Boolean], thenBody: Expr[T], elseBody: Expr[T]) extends Expr[T]
  final case class Switch[R, T](access: Expr[R], cases: Seq[(Expr[Boolean], Expr[T])], defaultBody: Expr[T]) extends Expr[T]
  final case class BinaryLogical(left: Expr[Boolean], right: Expr[Boolean], operator: BinaryLogicalOperator) extends Expr[Boolean]
  final case class UnaryLogical(expr: Expr[Boolean]) extends Expr[Boolean]
  final case class GetParam[T](id: String, indices: Seq[Int]) extends Expr[T]
  final case class GetVar[T](id: String, indices: Seq[Int]) extends Expr[T]
  final case class Sum(condition: Expr[Boolean], body: Expr[Double]) extends Expr[Double]

  implicit class ExprPredef[T](expr: Expr[T]) {}

  implicit def fromVar[T](v: Variable[T, Unit]): Expr[T] = ???
  implicit def int2double(e: Expr[Int]): Expr[Double] = ???
  implicit def double2Int(e: Expr[Double]): Expr[Int] = ???
  implicit def bool2Int(e: Expr[Boolean]): Expr[Int] = ???
  implicit def bool2Double(e: Expr[Boolean]): Expr[Double] = ???
  implicit def mkFromInt(v: Int): Expr[Int] = ???

}
