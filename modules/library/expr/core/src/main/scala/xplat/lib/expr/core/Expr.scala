package xplat.lib.expr.core

import scala.language.implicitConversions

sealed trait Expr[T] {
  def ast: Ast
  def ===(other: Expr[T]): Expr[Boolean] = Expr.Binary(this.ast, other.ast, Operation.Boolean.EQUALS)
}

object Expr {


  final case class Binary[T](left: Ast, right: Ast, operation: Operation) extends Expr[T] {
    override def ast: Ast = Ast.BinaryOperation(left, operation, right)
  }

  final case class Unnary[T](expr: Expr[T], operation: Operation) extends Expr[T] {
    override def ast: Ast = Ast.UnaryOperation(operation, expr.ast)
  }

  final case class Access[T](name: String, entity: String, index: Int, mapper: TypeMapper[T]) extends Expr[T] {
    override def ast: Ast = Ast.FieldAccess(entity, name, index)
  }

  final case class EString(value: String) extends Expr[String] {
    override def ast: Ast = Ast.Literal.LString(value)
  }

  final case class EBoolean(value: Boolean, ast: Ast) extends Expr[Boolean]

  final case class EInt(value: Int) extends Expr[Int] {
    override def ast: Ast = Ast.Literal.LInt(value)
  }

  final case class Wrapper[T](ast: Ast) extends Expr[T] {}

  object EBoolean {
    def apply(value: Boolean): EBoolean = EBoolean(value, Ast.Literal.LBoolean(value))
  }

  implicit def mkString(str: String): Expr[String] = EString(str)
  implicit def mkInt(value: Int): Expr[Int] = EInt(value)
  implicit def mkBoolean(value: Boolean): Expr[Boolean] = EBoolean(value)

  implicit class BooleanPredef(left: Expr[Boolean]) {
    def &&(right: Expr[Boolean]): Expr[Boolean] = Expr.Binary(left.ast, right.ast, Operation.Boolean.AND)
    def ||(right: Expr[Boolean]): Expr[Boolean] = Expr.Binary(left.ast, right.ast, Operation.Boolean.OR)
  }

  implicit class StringPredef(left: Expr[String]) {
    def *(right: Expr[Int]): Expr[String] = ???
  }

}
