package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}


case object IsAdditionFactor {

  def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical, Double, Expr.Numerical)] = {
    expr match {
      case IsBinary(WithConstantFactor(f1, e1), IsAddition(op), WithConstantFactor(f2, e2)) =>
        val sign = {
          op match {
            case Arithmentics.Plus  => 1
            case Arithmentics.Minus => -1
          }
        }
        Option((f1, e1, sign * f2, e2))
      case _ => None
    }
  }

}
