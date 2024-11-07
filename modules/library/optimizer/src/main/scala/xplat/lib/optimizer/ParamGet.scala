package xplat.lib.optimizer

trait ParamGet[Domain, Out] {
  type Apply <: Product
  def apply(a: Apply): Expr[Out]
}

object ParamGet extends ParamGetImpl {
  type Aux[Domain,Out, Apply0] = ParamGet[Domain, Out] { type Apply = Apply0 }
}
