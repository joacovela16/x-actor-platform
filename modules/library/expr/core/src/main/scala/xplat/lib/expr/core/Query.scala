package xplat.lib.expr.core

trait Query[Q] {
  type R
  def value: Q
  def construct(q: Q, de: DataExtractor): R = ???
  def ast: AST
  def constructFromAST(ast: AST): Q
  def getAST(q: Q): AST
}

object Query extends QueryImplicits {
  type Aux[Q, R0] = Query[Q] { type R = R0 }

  trait MatQuery[Q, R0] extends Query[Q] {
    override type R = R0
  }

  def apply[M[_[_]], F[_], G[_]](a: AST, v: M[F]): Query.Aux[M[F], M[G]] = new Query[M[F]] {
    override type R = M[G]
    override def value: M[F] = v
    override def ast: AST = a
    override def constructFromAST(ast: AST): M[F] = v
    override def getAST(q: M[F]): AST = a
  }

}
