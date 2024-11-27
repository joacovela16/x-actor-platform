package xplat.lib.optimizer.model.dsl

sealed trait BinaryLogicOps

object BinaryLogicOps {
  case object And extends BinaryLogicOps
  case object Or extends BinaryLogicOps
}
