package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr


case object IsParam {

  def unapply(expr: Expr.Numerical): Option[Expr.Numerical.GetParam] = {
    expr match {
      case e: Expr.Numerical.GetParam => Option(e)
      case _                          => None
    }
  }

}
