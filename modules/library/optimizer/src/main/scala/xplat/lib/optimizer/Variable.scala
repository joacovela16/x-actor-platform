package xplat.lib.optimizer

final case class Variable[T, D](
    lowerBound: Option[Double],
    upperBound: Option[Double]
) extends ExprOps[T] {
  def domain[D2](value: D2)(implicit ops: DomainOperation[D2, T]): Variable[T, ops.Tuple] = ???

}

object Variable {

  final case class Get[T]()

  implicit class Predef[
      T,
      D,
      Tuple,
      TupleExpr,
      Apply,
      Conditional
  ](value: Variable[T, Tuple])(implicit
      ops: DomainOperation.Aux[D, T, Tuple, TupleExpr, Apply, Conditional]
  ) {
    def where(a: Conditional): Variable[T, Tuple] = ???
    //    def baseOn(value: D): Param[D] = ???
  }

  implicit class GetPredef[T, D, Apply](param: Variable[T, D])(implicit get: VariableGet.Aux[D, T, Apply]) {
    def apply(a: Apply): Expr[T] = ???
  }

}
