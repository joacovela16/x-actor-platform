package xplat.lib.optimizer

final case class SumExpr[D]() {}

object SumExpr {

  implicit class Predef[D, Tuple, TupleExpr, Apply, Conditional](sumExpr: SumExpr[D])(implicit ops: SetContainerOps.Aux[D, Double, Tuple, TupleExpr, Apply, Conditional]) {
    def apply(a: Conditional): Dummy[D, Apply] = ???
  }

  final case class Dummy[D, -Apply](sumExpr: SumExpr[D], dummy:Boolean=true) {
    def apply(a: Apply): Expr[Double] = ???
  }

}
