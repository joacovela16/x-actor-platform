package xplat.lib.optimizer

trait DomainOperation[Domain] {
  type Tuple
  type Apply
  type Conditional
  def asReference(d: Domain): Seq[String]
  def apply(a: Apply): Context => Expr.Numerical
  def conditional(a: Conditional): Context => Expr.Logical
}

object DomainOperation extends DomainOperationImpl {

  type Aux[
      Domain,
      Tuple0,
      Apply0,
      Conditional0
  ] = DomainOperation[Domain] {
    type Tuple = Tuple0;
    type Apply = Apply0;
    type Conditional = Conditional0;
  }

}
