package xplat.lib.optimizer.model

final case class SubjectTo[D](domain: D)

object SubjectTo {

  implicit class Predef[D, T, Apply, Cond](
      subjectTo: SubjectTo[D]
  )(implicit ops: DomainOperation.Aux[D, T, Apply, Cond]) {
    def apply(x: Cond): Expr.Logical = Expr.Logical.Forall(ops.asReference(subjectTo.domain), true, ops.conditional(x))

    def where(x: Cond)(y: Cond): Expr.Logical = {
      Expr.Logical.Forall(ops.asReference(subjectTo.domain), ops.conditional(x), ops.conditional(y))
    }

  }

}
