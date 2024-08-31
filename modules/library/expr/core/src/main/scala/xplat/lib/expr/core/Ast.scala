package xplat.lib.expr.core

sealed trait Ast {}

object Ast {
  case object Empty extends Ast
  final case class Entity(tpe: EType.Clazz) extends Ast
  final case class FieldAccess(entity: String, field: String, index: Int) extends Ast
  final case class BinaryOperation(left: Ast, operation: Operation, right: Ast) extends Ast
  final case class UnaryOperation(adt: Ast, operation: Operation) extends Ast
  final case class Apply(name: String, over: Ast, body: Ast = Ast.Empty) extends Ast
  final case class Tuple(items: Ast*) extends Ast

  object Apply {
    //      def empty(name: String): Function = Function.apply(name, Nil, Map.empty, ADT.Empty)
    //      def apply(name: String)(props: (String, ADT)*): Function = Function(name, Nil, props.toMap, ADT.Empty)
    //      def apply(name: String, args: ADT*): Function = Function(name, args.toList, Map.empty, ADT.Empty)
  }

  sealed trait Literal extends Ast {
    def tpe: EType
  }

  object Literal {
    final case class LInt(data: Int, tpe: EType = EType.Int) extends Literal
    final case class LFloat(data: Float, tpe: EType = EType.Int) extends Literal
    final case class LBoolean(data: Boolean, tpe: EType = EType.Int) extends Literal
    final case class LString(data: String, tpe: EType = EType.Int) extends Literal
    final case class LByteArray(data: Array[Byte], tpe: EType = EType.Int) extends Literal
  }

}
