package xplat.lib.expr.core

import xplat.lib.expr.core.Commons.Id
import xplat.lib.expr.core.Query.Aux

abstract class Table[M[_[_]]]()(implicit md: Metadata[M]) {
  def query: Aux[M[TAst], M[Id]] = md.query
  def tpe: EType.Clazz = md.tpe

  implicit def selfContainer: Query.Aux[M[TAst], M[Commons.Id]] = md.query

//  def select: Select[M[TAst], M[Id]]
//  def join(f: M[TAst] => TAst[Boolean]) = select.join(f)
  def print(): Unit = {
    println(md.tpe)
  }

}
