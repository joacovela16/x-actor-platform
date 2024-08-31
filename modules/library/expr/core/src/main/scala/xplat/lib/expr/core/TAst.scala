package xplat.lib.expr.core

import scala.language.implicitConversions

sealed trait TAst[T] {
  def ast: Ast
  def ===(other: TAst[T]): TAst[Boolean] = TAst.Binary(this.ast, other.ast, Operation.Boolean.EQUALS)
}

object TAst {

  final case class Block[T](ast: Ast) extends TAst[T]

  //    final case class Function[T](name: String, args: List[ADT], props: Map[String, ADT], result: ADT) extends TAst[T] {
  //      override def adt: ADT = ADT.Apply(name, args, props, ADT.Empty)
  //    }

  final case class Binary[T](left: Ast, right: Ast, operation: Operation) extends TAst[T] {
    override def ast: Ast = Ast.BinaryOperation(left, operation, right)
  }

  final case class Unnary[T](expr: TAst[T], operation: Operation) extends TAst[T] {
    override def ast: Ast = Ast.UnaryOperation(expr.ast, operation)
  }

  final case class Access[T](name: String, entity: String, index: Int, mapper: TypeMapper[T]) extends TAst[T] {
    override def ast: Ast = Ast.FieldAccess(entity, name, index)
  }

  final case class EString(value: String) extends TAst[String] {
    override def ast: Ast = Ast.Literal.LString(value)
  }

  final case class EBoolean(value: Boolean, ast: Ast) extends TAst[Boolean]

  final case class EInt(value: Int) extends TAst[Int] {
    override def ast: Ast = Ast.Literal.LInt(value)
  }

  final case class Wrapper[T](ast: Ast) extends TAst[T] {}

  object EBoolean {
    def apply(value: Boolean): EBoolean = EBoolean(value, Ast.Literal.LBoolean(value))
  }

  implicit def mkString(str: String): TAst[String] = EString(str)
  implicit def mkInt(value: Int): TAst[Int] = EInt(value)
  implicit def mkBoolean(value: Boolean): TAst[Boolean] = EBoolean(value)

  implicit class BooleanPredef(left: TAst[Boolean]) {
    def &&(right: TAst[Boolean]): TAst[Boolean] = TAst.Binary(left.ast, right.ast, Operation.Boolean.AND)
    def ||(right: TAst[Boolean]): TAst[Boolean] = TAst.Binary(left.ast, right.ast, Operation.Boolean.OR)
  }

  implicit class StringPredef(left: TAst[String]) {
    def *(right: TAst[Int]): TAst[String] = ???
  }

}
