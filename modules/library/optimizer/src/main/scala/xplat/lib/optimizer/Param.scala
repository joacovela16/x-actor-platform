package xplat.lib.optimizer

final case class Param[T, D](kind: Kind, domain: Domain[D], criteria: Expr[T]) {
  def domain[D2 ](value: D2)(implicit ops: SetContainerOps[D2, T]): Param[T, ops.Tuple] = ???
}

object Param {

  implicit class MapPredef[
      T,
      D ,
      Tuple,
      TupleExpr,
      Apply,
      Conditional
  ](param: Param[T, Tuple])(implicit
      ops: SetContainerOps.Aux[D, T, Tuple, TupleExpr, Apply, Conditional]
  ) {
    def project(a: Apply): Param[T, Tuple] = ???
//    def baseOn(value: D): Param[D] = ???
  }

  implicit class GetPredef[T, D, Apply](param: Param[T, D])(implicit get: ParamGet.Aux[D, T, Apply]) {
    def apply(a: Apply): Expr[T] = get(a)
  }

  final case class BaseOn()
}
