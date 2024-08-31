package xplat.lib.expr.core

abstract class Table[M[_[_]]]()(implicit md: Metadata[M]) {
  implicit def selfContainer: Query.Aux[M[TAst], M[Commons.Id]] = md.query
//  def select: Select[M[TAst], M[Id]]
//  def join(f: M[TAst] => TAst[Boolean]) = select.join(f)
  def print(): Unit = {
    println(md.tpe)
  }
}
