package xplat.lib.expr.core

trait Query[Q] {
  type R
  def value: Q
  def construct(q: Q, de: DataExtractor): R = ???
  def ast: Ast
  def constructFromAST(ast: Ast): Q
  def getAST(q: Q): Ast
}

object Query extends QueryImplicits {
  type Aux[Q, R0] = Query[Q] { type R = R0 }

  trait MatQuery[Q, R0] extends Query[Q] {
    override type R = R0
  }

  def apply[M[_[_]], F[_], G[_]](a: Ast, v: M[F]): Query.Aux[M[F], M[G]] = new Query[M[F]] {
    override type R = M[G]
    override def value: M[F] = v
    override def ast: Ast = a
    override def constructFromAST(ast: Ast): M[F] = v
    override def getAST(q: M[F]): Ast = a
  }

}
