package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}


case object FactorExpr {

  def unapply(expr: Expr.Numerical): Option[(Expr.Numerical, Arithmentics.Factor, Expr.Numerical)] = {
    expr match {
      case Expr.Numerical.Binary(left, IsFactor(op), right) =>
        Option((left, op, right))
      case _ =>
        None
    }
  }

}
