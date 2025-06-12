package xplat.lib.expr.core

import xplat.lib.expr.core.Commons.Id

abstract class Table[M[_[_]]]()(implicit md: Metadata[M]) {
  private val init = md.query()
  def query(): Query.Aux[M[Expr], M[Id]] = md.query()

  def tpe: Type.Clazz = md.tpe
  implicit def selfContainer: Query.Aux[M[Expr], M[Commons.Id]] = init
}
