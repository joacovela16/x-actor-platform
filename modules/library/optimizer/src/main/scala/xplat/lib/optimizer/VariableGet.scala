package xplat.lib.optimizer

trait VariableGet[Domain] {
  type Apply
  def apply(a: Apply): Seq[Int]
}

object VariableGet extends VarGetImpl {
  type Aux[Domain, Apply0] = VariableGet[Domain] { type Apply = Apply0 }
}
