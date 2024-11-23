package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr

case object IsLiteral {

  def unapply(expr: Expr.Numerical): Option[Double] = {
    expr match {
      case Expr.Numerical.Constant(data) => Option(data)
      case _                        => None
    }
  }

}
