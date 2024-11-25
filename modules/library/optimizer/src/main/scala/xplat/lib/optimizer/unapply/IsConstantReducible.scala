package xplat.lib.optimizer.unapply

import xplat.lib.optimizer.model.dsl.Expr

case object IsConstantReducible {

  def unapply(expr: Expr.Numerical): Option[Expr.Numerical] = {
    expr match {
      case IsLiteral(_) =>
        Option(expr)
      case IsBinary(IsConstantReducible(_), _, IsConstantReducible(_)) =>
        Option(expr)
      case _ =>
        None
    }
  }

}
