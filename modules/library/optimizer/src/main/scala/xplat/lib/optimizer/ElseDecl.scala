package xplat.lib.optimizer

import com.softwaremill.quicklens.ModifyPimp

final case class ElseDecl(cond: Expr.Logical, elseIf: Seq[(Expr.Logical, Expr.Numerical)], thenBody: Expr.Numerical) {
  def Else(elseBody: Expr.Numerical): Expr.Statement = Expr.Statement.If(cond, thenBody, elseIf, elseBody)
  def ElseIf(cond: Expr.Logical)(condBody: Expr.Numerical): ElseDecl = this.modify(_.elseIf).using(xs => xs :+ cond -> condBody)
}
