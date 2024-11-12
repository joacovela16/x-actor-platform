package xplat.lib.optimizer

import com.softwaremill.quicklens.ModifyPimp

final case class Model(
    kind: ObjectiveKind,
    objective: Expr.Numerical,
    constraints: Seq[Constraint]
) {

  def subjectTo(v: Expr.Logical*): Model = {
    this.modify(_.constraints).using(xs => xs ++ v.map(x => Constraint(x)))
  }

}
