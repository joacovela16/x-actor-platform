package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}


case object IsBinary {

  def unapply(expr: Expr.Numerical): Option[(Expr.Numerical, Arithmentics, Expr.Numerical)] = {
    expr match {
      case Expr.Numerical.Binary(left, operator, right) =>
        Option((left, operator, right))
      case _ =>
        None
    }
  }

}
