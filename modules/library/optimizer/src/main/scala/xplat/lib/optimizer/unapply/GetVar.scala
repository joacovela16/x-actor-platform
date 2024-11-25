package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr

case object GetVar {

  def unapply(expr: Expr.Numerical): Option[String] = {
    expr match {
      case IsVar(a) =>
        Option(a.id)
      case IsBinary(a, _, b) =>
        unapply(a).orElse(unapply(b))
      case _ =>
        None
    }
  }

}
