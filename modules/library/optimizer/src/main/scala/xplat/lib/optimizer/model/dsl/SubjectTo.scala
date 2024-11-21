package xplat.lib.optimizer.model.dsl

final case class SubjectTo[D](domain: D)

object SubjectTo {

  implicit class Predef[D, T, Apply, Cond](
      subjectTo: SubjectTo[D]
  )(implicit ops: DomainOperation.Aux[D, T, Apply, Cond]) {

    def apply(x: Cond): Expr.Constraint = {
      val ref = ops.asReference(subjectTo.domain)
      Expr.Constraint.Forall(ops.asReference(subjectTo.domain), true, ops.conditional(x)(ref))
    }

    def where(x: Cond)(y: Cond): Expr.Constraint = {
      val ref = ops.asReference(subjectTo.domain)
      Expr.Constraint.Forall(ops.asReference(subjectTo.domain), ops.conditional(x)(ref), ops.conditional(y)(ref))
    }

  }

}
