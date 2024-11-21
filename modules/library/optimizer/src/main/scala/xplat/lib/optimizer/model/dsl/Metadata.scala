package xplat.lib.optimizer.model.dsl

sealed trait Metadata {

}

object Metadata {
  case object VarRef extends Metadata
  case object DivisionByVar extends Metadata
  case object VarMultiplication extends Metadata
  case object NoLinear extends Metadata
}
