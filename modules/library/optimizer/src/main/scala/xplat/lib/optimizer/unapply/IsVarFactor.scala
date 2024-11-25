package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}

case object IsVarFactor {

  def unapply(expr: Expr.Numerical): Option[(Expr.Numerical, Expr.Numerical.GetVar)] = {
    expr match {
      case IsVar(e) =>
        Option((1, e))
      case IsBinary(IsConstant(a), Arithmentics.Times, IsVar(b)) =>
        Option(a, b)
      case IsBinary(IsVar(b), IsFactor(_), IsConstant(a)) =>
        Option(a, b)
      case IsBinary(IsVar(b), Arithmentics.Divide, IsConstant(a)) =>
        Option(1 / a, b)
      case IsBinary(IsConstant(a), Arithmentics.Times, IsVarFactor(b, c)) =>
        // 2 * (x/5) => 2/5 -> x
        Option(a * b, c)
      case _ =>
        None
    }
  }

}
