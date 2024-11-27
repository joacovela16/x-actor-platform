package xplat.lib.optimizer.model.dsl

trait DomainOperation[Domain] {
  type Tuple
  type Numerical
  type Conditional
  type Relation
  def asReference(d: Domain): Seq[SetDecl]
  def numerical(a: Numerical): Seq[SetDecl] => Expr.Numerical
  def conditional(a: Conditional): Seq[SetDecl] => Expr.Predicate.Logical
  def relation(a: Relation): Seq[SetDecl] => Expr.Predicate.Relation
}

object DomainOperation extends DomainOperationImpl {

  type Aux[
      Domain,
      Tuple0,
      Numerical0,
      Conditional0,
      Relation0
  ] = DomainOperation[Domain] {
    type Tuple = Tuple0;
    type Numerical = Numerical0;
    type Conditional = Conditional0;
    type Relation = Relation0;
  }

}
