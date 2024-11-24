package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr


case object IsNotVar {

  def unapply(expr: Expr.Numerical): Option[Expr.Numerical] = {
    expr match {
      case _: Expr.Numerical.GetVar => None
      case e => Option(e)
    }
  }

}
