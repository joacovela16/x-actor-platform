package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr

object IsTerm {

  def unapply(expr: Expr.Numerical): Option[Expr.Numerical] = {
    expr match {
      case IsVarFactorExpr(_, _, e) =>
        Option(e)
      case IsBinary(IsConstant(_), IsFactor(_), IsConstant(_)) =>
        Option(expr)
      case _ =>
        None
    }
  }

}
