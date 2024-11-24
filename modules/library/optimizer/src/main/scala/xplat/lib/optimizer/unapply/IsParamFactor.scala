package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}

case object IsParamFactor {

  def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical.GetParam)] = {
    expr match {
      case e: Expr.Numerical.GetParam => Option(1.0 -> e)
      case IsBinary(IsLiteral(c), Arithmentics.Times, IsParam(e)) =>
        Option(c -> e)
      case IsBinary(IsParam(e), Arithmentics.Times, IsLiteral(c)) =>
        Option(c -> e)
      case _ => None
    }
  }

}
