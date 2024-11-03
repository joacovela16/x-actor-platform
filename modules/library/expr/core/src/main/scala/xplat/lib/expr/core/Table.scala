package xplat.lib.expr.core

import xplat.lib.expr.core.Commons.Id

abstract class Table[M[_[_]]]()(implicit md: Metadata[M]) {
  def query: Query.Aux[M[Expr], M[Id]] = md.query
  def tpe: Type.Clazz = md.tpe

  implicit def selfContainer: Query.Aux[M[Expr], M[Commons.Id]] = md.query

//  def select: Select[M[TAst], M[Id]]
//  def join(f: M[TAst] => TAst[Boolean]) = select.join(f)
  def print(): Unit = {
    println(md.tpe)
  }

}
