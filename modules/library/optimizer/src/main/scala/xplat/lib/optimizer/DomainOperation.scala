package xplat.lib.optimizer

trait DomainOperation[Domain, Out] {
  type Tuple
  type TupleExpr
  type Apply
  type Conditional
  def apply(a: Apply): TupleExpr => Expr[Out]
  def conditional(a: Conditional): TupleExpr => Expr[Boolean]
}

object DomainOperation extends DomainOperationImpl {
  type Aux[
    T,
    Out,
    Tuple0,
    TupleExpr0,
    Apply0,
    Conditional0,
  ] = DomainOperation[T, Out] {
    type Tuple = Tuple0;
    type Apply = Apply0;
    type TupleExpr = TupleExpr0;
    type Conditional = Conditional0;
  }
}
