package xplat.lib.expr.core

sealed trait TypedAst[T] {
  def ast: AST
  def ===(other: TypedAst[T]): TypedAst[Boolean] = TypedAst.Binary(this.ast, other.ast, Operation.Boolean.EQUALS)
}

object TypedAst {

  final case class Block[T](ast: AST) extends TypedAst[T]

  //    final case class Function[T](name: String, args: List[ADT], props: Map[String, ADT], result: ADT) extends TypedAst[T] {
  //      override def adt: ADT = ADT.Apply(name, args, props, ADT.Empty)
  //    }

  final case class Binary[T](left: AST, right: AST, operation: Operation) extends TypedAst[T] {
    override def ast: AST = AST.BinaryOperation(left, operation, right)
  }

  final case class Unnary[T](expr: TypedAst[T], operation: Operation) extends TypedAst[T] {
    override def ast: AST = AST.UnaryOperation(expr.ast, operation)
  }

  final case class Access[T](name: String, entity: String, index: Int, mapper: TypeMapper[T]) extends TypedAst[T] {
    override def ast: AST = AST.FieldAccess(entity, name, index)
  }

  final case class EString(value: String) extends TypedAst[String] {
    override def ast: AST = AST.Literal.LString(value)
  }

  final case class EBoolean(value: Boolean, ast: AST) extends TypedAst[Boolean]

  final case class EInt(value: Int) extends TypedAst[Int] {
    override def ast: AST = AST.Literal.LInt(value)
  }

  final case class Wrapper[T](ast: AST) extends TypedAst[T] {}

  object EBoolean {
    def apply(value: Boolean): EBoolean = EBoolean(value, AST.Literal.LBoolean(value))
  }

  implicit def mkString(str: String): TypedAst[String] = EString(str)
  implicit def mkInt(value: Int): TypedAst[Int] = EInt(value)
  implicit def mkBoolean(value: Boolean): TypedAst[Boolean] = EBoolean(value)

  implicit class BooleanPredef(left: TypedAst[Boolean]) {
    def &&(right: TypedAst[Boolean]): TypedAst[Boolean] = TypedAst.Binary(left.ast, right.ast, Operation.Boolean.AND)
    def ||(right: TypedAst[Boolean]): TypedAst[Boolean] = TypedAst.Binary(left.ast, right.ast, Operation.Boolean.OR)
  }

  implicit class StringPredef(left: TypedAst[String]) {
    def *(right: TypedAst[Int]): TypedAst[String] = ???
  }

}
