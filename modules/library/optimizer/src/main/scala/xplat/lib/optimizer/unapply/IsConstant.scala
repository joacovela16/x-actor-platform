package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr


case object IsConstant {

  def unapply(expr: Expr.Numerical): Option[Expr.Numerical] = {
    expr match {
      case Expr.Numerical.GetParam(_, _) =>
        Option(expr)
      case Expr.Numerical.Constant(_) =>
        Option(expr)
      case _ =>
        None
    }
  }

}
