package xplat.lib.optimizer.model.dsl

import com.softwaremill.quicklens.ModifyPimp

final case class ElseDecl(cond: Expr.Predicate, elseIf: Seq[(Expr.Predicate, Expr.Numerical)], thenBody: Expr.Numerical) {
  def Else(elseBody: Expr.Numerical): Expr.Statement = Expr.Statement.If(cond, thenBody, elseIf, elseBody)
  def ElseIf(cond: Expr.Predicate)(condBody: Expr.Numerical): ElseDecl = this.modify(_.elseIf).using(xs => xs :+ cond -> condBody)
}
