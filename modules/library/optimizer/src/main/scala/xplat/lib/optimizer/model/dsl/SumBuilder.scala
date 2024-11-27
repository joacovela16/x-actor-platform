package xplat.lib.optimizer.model.dsl

final case class SumBuilder[D](domain: D)

object SumBuilder {

  implicit class Predef[D, Tuple, Apply, Conditional, Relation](data: SumBuilder[D])(implicit ops: DomainOperation.Aux[D, Tuple, Apply, Conditional, Relation]) {

    def where(a: Conditional): Dummy[D, Tuple, Apply, Conditional, Relation] = {
      Dummy(data.domain, a, ops)
    }

    def apply(a: Apply): Expr.Numerical = {
      val ref = ops.asReference(data.domain)
      Expr.Numerical.Summatory(ref, true, ops.numerical(a)(ref))
    }

  }

  final case class Dummy[D, Tuple, Apply, Conditional, Relation](data: D, cond: Conditional, ops: DomainOperation.Aux[D, Tuple, Apply, Conditional, Relation]) {

    def apply(a: Apply): Expr.Numerical = {
      val ref = ops.asReference(data)
      Expr.Numerical.Summatory(ops.asReference(data), ops.conditional(cond)(ref), ops.numerical(a)(ref))
    }

  }

}
