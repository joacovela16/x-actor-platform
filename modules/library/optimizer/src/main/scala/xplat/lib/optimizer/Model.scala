package xplat.lib.optimizer

final case class Model(
    kind: ObjectiveKind,
    objective: Expr[Double],
    constraints: Seq[Constraint]
) {
  def apply(values: Constraint*): Model = ???

  def subjectTo(values: Constraint*): Model = {
    ???
  }

}
