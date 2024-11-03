package xplat.lib.expr.sql.base

import xplat.lib.expr.core._

final case class Select[Q, R](query: Query.Aux[Q, R]) extends QueryProvider[Q]

object Select {

  trait Operations {

    implicit class TablePredef[M[_[_]]](table: Table[M]) {
      def select: Select[M[Expr], M[Commons.Id]] = Select(table.query)
    }

  }

  trait Filter {

    implicit class FilterOps[Q, R](s: Select[Q, R]) {
      private val query = s.query

      def withFilter(f: Q => Expr[Boolean]): Select[Q, R] = filter(f)

      def filter(f: Q => Expr[Boolean]): Select[Q, R] = {
        val localExpr: Expr[Boolean] = f(query.value)

        Select[Q, R](
          query = new Query.MatQuery[Q, R] {
            self =>
            override def value: Q = query.value
            override def ast: Ast = Ast.Apply("filter", query.ast, localExpr.ast)
            override def getAST(q: Q): Ast = Ast.Apply("filter", query.getAST(q), localExpr.ast)
            override def constructFromAST(ast: Ast): Q = query.constructFromAST(ast)
          }
        )
      }

    }

  }

  trait Map {

    implicit class MapOps[Q, R](s: Select[Q, R]) {
      private val query = s.query

      def map[Q1](f: Q => Q1)(implicit otherQuery: Query[Q1]): Select[Q1, otherQuery.R] = {
        Select[Q1, otherQuery.R](
          query = new Query.MatQuery[Q1, otherQuery.R] {
            self =>
            override def value: Q1 = f(query.value)
            override def constructFromAST(ast: Ast): Q1 = otherQuery.constructFromAST(ast)
            override def getAST(q: Q1): Ast = otherQuery.getAST(q)
            override def ast: Ast = Ast.Apply("map", query.ast, otherQuery.getAST(self.value))
          }
        )
      }

    }

  }

  trait FlatMap {

    implicit class FlatMapPredef[Q, R](select: Select[Q, R]) {
      private val query = select.query

      def flatMap[Q1, R1](f: Q => Select[Q1, R1]): Select[Q1, R1] = {
        val localValue = f(query.value)
        val otherQuery = localValue.query

        Select[Q1, R1](
          query = new Query[Q1] {
            override type R = R1

            override def value: Q1 = otherQuery.value

            override def ast: Ast = Ast.Apply("flatMap", query.ast, otherQuery.ast)

            override def constructFromAST(ast: Ast): Q1 = otherQuery.constructFromAST(ast)

            override def getAST(q: Q1): Ast = otherQuery.getAST(q)
          }
        )
      }

    }

  }

  trait JoinOverTable {

    implicit class TableJoinPredef[Q, R0](select: Select[Q, R0]) {
      private val query = select.query

      def join(f: Q => Expr[Boolean]): Select[Q, R0] = {
        val localExpr = f(query.value)

        Select[Q, query.R](
          query = new Query.MatQuery[Q, query.R] {
            override def value: Q = query.value
            override def ast: Ast = Ast.Apply("join", query.ast, localExpr.ast)
            override def constructFromAST(ast: Ast): Q = query.constructFromAST(ast)
            override def getAST(q: Q): Ast = Ast.Apply("join", query.getAST(q), localExpr.ast)
          }
        )
      }

      def join[G[_[_]]](other: Table[G])(f: (Q, G[Expr]) => Expr[Boolean]): Select[(Q, G[Expr]), (R0, G[Commons.Id])] = {

        val g = other.selfContainer.value
        val expr = f(query.value, g)

        Select[(Q, G[Expr]), (R0, G[Commons.Id])](
          query = new Query.MatQuery[(Q, G[Expr]), (R0, G[Commons.Id])] {
            self =>
            override def value: (Q, G[Expr]) = (query.value, g)

            override def ast: Ast = Ast.Apply(
              name = "join",
              over = Ast.Tuple(query.ast, other.selfContainer.ast),
              body = expr.ast
            )

            override def constructFromAST(ast: Ast): (Q, G[Expr]) = (query.constructFromAST(ast), other.selfContainer.constructFromAST(ast))

            override def getAST(q: (Q, G[Expr])): Ast = self.ast
          }
        )
      }

    }

  }

}
