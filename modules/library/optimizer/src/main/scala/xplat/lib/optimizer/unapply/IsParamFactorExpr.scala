package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}

case object IsParamFactorExpr {

  def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical.GetParam, Expr.Numerical)] = {
    expr match {
      case e: Expr.Numerical.GetParam => Option(1.0, e, expr)
      case IsBinary(IsLiteral(c), Arithmentics.Times, IsParam(e)) =>
        Option(c, e, e)
      case IsBinary(IsParam(e), Arithmentics.Times, IsLiteral(c)) =>
        Option(c, e, e)
      case _ =>
        None
    }
  }

}
