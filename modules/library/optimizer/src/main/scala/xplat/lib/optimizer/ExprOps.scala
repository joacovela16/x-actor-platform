package xplat.lib.optimizer

trait ExprOps[T] {
  def &&(other: Expr[Boolean]): Expr[Boolean] = ???
  def ||(other: Expr[Boolean]): Expr[Boolean] = ???
  def ===(other: Expr[T]): Expr[Boolean] = ???
  def >(other: Expr[T]): Expr[Boolean] = ???
  def >=(other: Expr[T]): Expr[Boolean] = ???
  def <(other: Expr[T]): Expr[Boolean] = ???
  def <=(other: Expr[T]): Expr[Boolean] = ???
  def +(other: Expr[T]): Expr[T] = ???
  def -(other: Expr[T]): Expr[T] = ???
  def /(other: Expr[T]): Expr[T] = ???
//  def -[A](other: A): Expr[A] = ???
//  def /[A](other: A): Expr[A] = ???
//  def +[A](other: A): Expr[A] = ???
//  def *[A](other: A): Expr[A] = ???
//  def +[A](other: Variable[A, Unit]): Expr[A] = ???
}
