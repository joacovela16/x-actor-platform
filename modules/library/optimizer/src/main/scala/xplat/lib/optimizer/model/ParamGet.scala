package xplat.lib.optimizer.model

import xplat.lib.optimizer.model.ParamGetImpl

trait ParamGet[Domain] {
  type Apply
  def apply(a: Apply): Seq[Int]
}

object ParamGet extends ParamGetImpl {
  type Aux[Domain, Apply0] = ParamGet[Domain] { type Apply = Apply0 }
}
