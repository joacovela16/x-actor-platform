package xplat.lib.optimizer

trait ExprOps[T] {
  def &&(other: Expr[Boolean]): Expr[Boolean] = ???
  def ||(other: Expr[Boolean]): Expr[Boolean] = ???
  def ===(other: Expr[T]): Expr[Boolean] = ???
  def >=(other: Expr[T]): Expr[Boolean] = ???
  def <(other: Expr[T]): Expr[Boolean] = ???
  def <=[A](other: Expr[A]): Expr[Boolean] = ???
  def >(other: Expr[T]): Expr[Boolean] = ???
  def +[A](other: Expr[A]): Expr[A] = ???
  def -(other: Expr[T]): Expr[T] = ???
  def *(other: Expr[T]): Expr[T] = ???
  def /(other: Expr[T]): Expr[T] = ???
}

object ExprOps {}
