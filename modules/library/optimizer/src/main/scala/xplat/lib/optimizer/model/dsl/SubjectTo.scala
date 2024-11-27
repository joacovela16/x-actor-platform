package xplat.lib.optimizer.model.dsl

final case class SubjectTo[D](domain: D)

object SubjectTo {

  implicit class Predef[D, T, Apply, Cond, Relation](
      subjectTo: SubjectTo[D]
  )(implicit ops: DomainOperation.Aux[D, T, Apply, Cond, Relation]) {

    def apply(x: Relation): Expr.Constraint = {
      val ref = ops.asReference(subjectTo.domain)
      Expr.Constraint.Forall(ops.asReference(subjectTo.domain), true, ops.relation(x)(ref))
    }

    def where(x: Cond)(y: Relation): Expr.Constraint = {
      val ref = ops.asReference(subjectTo.domain)
      Expr.Constraint.Forall(ops.asReference(subjectTo.domain), ops.conditional(x)(ref), ops.relation(y)(ref))
    }

  }

}
