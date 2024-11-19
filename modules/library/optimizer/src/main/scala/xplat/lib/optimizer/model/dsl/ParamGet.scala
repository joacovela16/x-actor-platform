package xplat.lib.optimizer.model.dsl

trait ParamGet[Domain] {
  type Apply
  def apply(a: Apply): Seq[Int]
}

object ParamGet extends ParamGetImpl {
  type Aux[Domain, Apply0] = ParamGet[Domain] { type Apply = Apply0 }
}
