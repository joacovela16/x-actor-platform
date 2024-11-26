package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr

case object IsNotReducible {

  def unapply(expr: Expr.Numerical): Boolean = {
    expr match {
      case IsVarFactor(_, _) => true
      case IsBinary(IsConstant(_), _, IsNotReducible()) =>
        true
      case IsBinary(IsNotReducible(), _, IsConstant(_)) =>
        true
      case IsBinary(IsNotReducible(), _, IsNotReducible()) =>
        true
      case _ =>
        false
    }
  }

}
