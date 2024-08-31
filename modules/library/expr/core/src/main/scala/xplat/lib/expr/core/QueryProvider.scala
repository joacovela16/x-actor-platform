package xplat.lib.expr.core

trait QueryProvider[Q] {
  type R = query.R
  val query: Query[Q]

  def toExpr: Q = query.constructFromAST(query.ast)
  def ast: Ast = query.ast
}
