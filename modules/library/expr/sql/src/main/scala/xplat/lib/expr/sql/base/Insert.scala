package xplat.lib.expr.sql.base

import xplat.lib.expr.core._

final case class Insert[Q, R](query: Query.Aux[Q, R]) extends QueryProvider[Q]

object Insert {

  final case class Batch[Q]() {
    def apply(f: (Q => SetCommand)*): Seq[Q => SetCommand] = f
  }

  trait SetCommand {
    type T
    def value: Expr[T]
    def ast: Ast
  }

  object SetCommand {
    type Aux[T0] = SetCommand { type T = T0 }

    def apply[T0](reference: Expr[T0], v: Expr[T0]): SetCommand.Aux[T0] = new SetCommand {
      override type T = T0
      override def value: Expr[T0] = v
//      override def typeMapper: TypeMapper[T0] = tm
      override def ast: Ast = Ast.BinaryOperation(reference.ast, Operation("set", ":="), v.ast)
    }

  }

  trait InsertEnabled {

    implicit class InsertInit[M[_[_]]](table: Table[M]) {
      def insert: Insert[M[Expr], M[Commons.Id]] = Insert(table.selfContainer)
    }

  }

  trait CommonInsert {

    implicit class FromExpr[T](reference: Expr[T]) {
      def :=(value: Expr[T]): SetCommand = SetCommand(reference, value)
    }

    implicit class CommonInsert[Q, R](insert: Insert[Q, R]) {
      private val query = insert.query

      def create(f: (Q => SetCommand)*): Insert[Q, R] = batch(b => b(f: _*))

      def batch(batchs: (Batch[Q] => Seq[Q => SetCommand])*): Insert[Q, R] = {

        val items = Ast.Tuple(batchs.map(x => Ast.Tuple(x(Batch()).map(y => y(query.value).ast).toList)).toList)
        Insert[Q, R](
          query = new Query.MatQuery[Q, R] {
            override def value: Q = query.value
            override def static: Ast = Ast.Apply("insert", query.static, items)
//            override def constructFromAST(ast: Ast): Q = query.constructFromAST(ast)
            override def dynamic(q: Q): Ast = Ast.Apply("insert", query.dynamic(q), items)
          }
        )
      }

    }

  }

}
