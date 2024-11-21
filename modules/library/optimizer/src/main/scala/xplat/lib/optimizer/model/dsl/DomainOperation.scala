package xplat.lib.optimizer.model.dsl

trait DomainOperation[Domain] {
  type Tuple
  type Apply
  type Conditional
  def asReference(d: Domain): Seq[SetDecl]
  def asStream(d: Domain, c: Context): LazyList[Context]
  def apply(a: Apply): Seq[SetDecl] => Expr.Numerical
  def conditional(a: Conditional): Seq[SetDecl] => Expr.Logical
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
