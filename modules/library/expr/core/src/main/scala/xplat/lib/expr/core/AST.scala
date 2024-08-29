package xplat.lib.expr.core

sealed trait AST {}

object AST {
  case object Empty extends AST
  final case class Entity(tpe: Type.Clazz) extends AST
  final case class FieldAccess(entity: String, field: String, index: Int) extends AST
  final case class BinaryOperation(left: AST, operation: Operation, right: AST) extends AST
  final case class UnaryOperation(adt: AST, operation: Operation) extends AST
  final case class Apply(name: String, over: AST, body: AST = AST.Empty) extends AST
  final case class Tuple(items: AST*) extends AST

  object Apply {
    //      def empty(name: String): Function = Function.apply(name, Nil, Map.empty, ADT.Empty)
    //      def apply(name: String)(props: (String, ADT)*): Function = Function(name, Nil, props.toMap, ADT.Empty)
    //      def apply(name: String, args: ADT*): Function = Function(name, args.toList, Map.empty, ADT.Empty)
  }

  sealed trait Literal extends AST {
    def tpe: Type
  }

  object Literal {
    final case class LInt(data: Int, tpe: Type = Type.Int) extends Literal
    final case class LFloat(data: Float, tpe: Type = Type.Int) extends Literal
    final case class LBoolean(data: Boolean, tpe: Type = Type.Int) extends Literal
    final case class LString(data: String, tpe: Type = Type.Int) extends Literal
    final case class LByteArray(data: Array[Byte], tpe: Type = Type.Int) extends Literal
  }

}
