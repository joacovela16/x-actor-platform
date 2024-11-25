package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}

case object WithAdditionFactor {

  def unapply(expr: Expr.Numerical): Option[(Expr.Numerical, Expr.Numerical, Expr.Numerical, Expr.Numerical)] = {
    expr match {
      case IsBinary(WithConstantFactor(f1, e1), IsAddition(op), WithConstantFactor(f2, e2)) =>
        val sign = {
          op match {
            case Arithmentics.Plus  => 1
            case Arithmentics.Minus => -1
          }
        }
        Option((f1, e1, if (sign == 1) f2 else sign * f2, e2))
      case IsBinary(e1, IsAddition(op), WithConstantFactor(f2, e2)) =>
        val sign = {
          op match {
            case Arithmentics.Plus  => 1
            case Arithmentics.Minus => -1
          }
        }
        Option((1, e1, sign * f2, e2))
      case IsBinary(WithConstantFactor(f2, e2), IsAddition(op), e1) =>
        val sign = {
          op match {
            case Arithmentics.Plus  => 1
            case Arithmentics.Minus => -1
          }
        }
        Option((sign * f2, e2, 1, e1))
      case _ => None
    }
  }

}
