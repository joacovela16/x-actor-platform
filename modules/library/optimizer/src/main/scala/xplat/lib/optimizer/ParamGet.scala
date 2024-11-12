package xplat.lib.optimizer

trait ParamGet[Domain] {
  type Apply <: Product
  def apply(a: Apply): Expr.Numerical
}

object ParamGet extends ParamGetImpl {
  type Aux[Domain, Apply0] = ParamGet[Domain] { type Apply = Apply0 }
}
