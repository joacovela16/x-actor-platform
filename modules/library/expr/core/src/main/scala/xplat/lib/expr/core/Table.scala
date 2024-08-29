package xplat.lib.expr.core

abstract class Table[M[_[_]]](implicit md: Metadata[M]) {
  implicit def selfContainer: Query.Aux[M[TypedAst], M[Commons.Id]] = md.query
//  def select: Select[M[TypedAst], M[Id]]
//  def join(f: M[TypedAst] => TypedAst[Boolean]) = select.join(f)
}
