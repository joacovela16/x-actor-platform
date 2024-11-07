package xplat.lib.optimizer

trait VariableGet[Domain, Out] {
  type Apply <: Product
  def apply(a: Apply): Expr[Out]
}

object VariableGet extends VarGetImpl {
  type Aux[Out,Domain, Apply0] = VariableGet[Out, Domain] { type Apply = Apply0 }
}
