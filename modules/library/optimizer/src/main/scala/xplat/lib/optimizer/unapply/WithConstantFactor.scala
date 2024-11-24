package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}

case object WithConstantFactor {

  def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical)] = {
    expr match {
      case IsBinary(IsLiteral(c), Arithmentics.Times, e) =>
        Option(c -> e)
      case IsBinary(e, Arithmentics.Times, IsLiteral(c)) =>
        Option(c -> e)
      case IsVarFactor(f, e) =>
        Option(f -> e)
      case IsParamFactor(f, e) =>
        Option(f -> e)
      case IsParam(e) =>
        Option(1.0 -> Expr.Numerical(e))

      case _ =>
        None
    }
  }

}
