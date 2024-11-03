package xplat.lib.expr.core

trait QueryImplicits {

  implicit def fromExpr[R0](implicit tm: TypeMapper[R0]): Query.Aux[Expr[R0], R0] = new Query[Expr[R0]] {
    override type R = R0
    override def value: Expr[R] = Expr.Wrapper(Ast.Empty)
    override def ast: Ast = Ast.Empty
    override def constructFromAST(ast: Ast): Expr[R] = Expr.Wrapper(ast)
    override def getAST(q: Expr[R]): Ast = q.ast
  }

  implicit def qt2[Q1, Q2](implicit q1: Query[Q1], q2: Query[Q2]): Query.Aux[(Q1, Q2), (q1.R, q2.R)] = {

    new Query[(Q1, Q2)] {
      override type R = (q1.R, q2.R)

      override def value: (Q1, Q2) = (q1.value, q2.value)

      override def ast: Ast = Ast.Tuple(q1.ast, q2.ast)

      override def getAST(q: (Q1, Q2)): Ast = Ast.Tuple(q1.getAST(q._1), q2.getAST(q._2))

      override def constructFromAST(ast: Ast): (Q1, Q2) = {
        (q1.constructFromAST(ast), q2.constructFromAST(ast))
      }
    }
  }
}
