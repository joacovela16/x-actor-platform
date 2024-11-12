package xplat.lib.optimizer

final case class SumBuilder[D](domain: D)

object SumBuilder {

  implicit class Predef[D, Tuple, Apply, Conditional](
      data: SumBuilder[D]
  )(implicit
      ops: DomainOperation.Aux[D, Tuple, Apply, Conditional]
  ) {

    def where(a: Conditional): Dummy[D, Tuple, Apply, Conditional] = {
      Dummy(data.domain, a, ops)
    }

    def apply(a: Apply): Expr.Numerical = {
      val ref = ops.asReference(data.domain)
      Expr.Numerical.Sum(ref, _ => true, ops(a))
    }
  }

  final case class Dummy[D, Tuple, Apply, Conditional](data: D, cond: Conditional, ops: DomainOperation.Aux[D, Tuple, Apply, Conditional]) {

    def apply(a: Apply): Expr.Numerical = {
      Expr.Numerical.Sum(ops.asReference(data), ops.conditional(cond), ops(a))
    }

  }

}
