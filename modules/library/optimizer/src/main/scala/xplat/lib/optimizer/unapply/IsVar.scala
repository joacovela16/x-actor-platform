package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr


case object IsVar {

  def unapply(expr: Expr.Numerical): Option[Expr.Numerical.GetVar] = {
    expr match {
      case e: Expr.Numerical.GetVar => Option(e)
      case _                        => None
    }
  }

}
