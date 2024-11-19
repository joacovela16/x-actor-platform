package xplat.lib.optimizer.model.dsl

import com.softwaremill.quicklens.ModifyPimp

final case class Model(
    kind: ObjectiveKind,
    objective: Expr.Numerical,
    constraints: Seq[Expr.Constraint]
) {

  def subjectTo(v: Expr.Constraint*): Model = this.modify(_.constraints).using(xs => xs ++ v.toSeq)

}
