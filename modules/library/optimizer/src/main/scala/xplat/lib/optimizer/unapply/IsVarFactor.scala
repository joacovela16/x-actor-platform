package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}


case object IsVarFactor {

  def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical.GetVar)] = {
    expr match {
      case IsVar(e) =>
        Option((1, e))
      case IsBinary(IsLiteral(a), Arithmentics.Times, IsVar(b)) =>
        Option(a -> b)
      case IsBinary(IsVar(b), IsFactor(_), IsLiteral(a)) =>
        Option(a -> b)
      case IsBinary(IsVar(b), Arithmentics.Divide, IsLiteral(a)) =>
        Option((1 / a) -> b)
      case IsBinary(IsLiteral(a), Arithmentics.Times, IsVarFactor(b, c)) =>
        // 2 * (x/5) => 2/5 -> x
        Option(a * b -> c)
      case _ =>
        None
    }
  }

}
