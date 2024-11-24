package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}

case object IsVarFactorExpr {

  def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical.GetVar, Expr.Numerical)] = {
    expr match {
      case IsVar(e) =>
        Option((1, e, expr))
      case IsBinary(IsLiteral(a), Arithmentics.Times, IsVar(b)) =>
        Option((a, b, expr))
      case IsBinary(IsVar(b), IsFactor(_), IsLiteral(a)) =>
        Option(a, b, expr)
      case IsBinary(IsVar(b), Arithmentics.Divide, IsLiteral(a)) =>
        Option(1 / a, b, expr)
      case IsBinary(IsLiteral(a), Arithmentics.Times, IsVarFactorExpr(b, c, e)) =>
        // 2 * (x/5) => 2/5 -> x
        Option(a * b, c, e)
      case _ =>
        None
    }
  }

}
