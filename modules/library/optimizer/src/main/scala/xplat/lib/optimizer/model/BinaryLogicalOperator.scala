package xplat.lib.optimizer.model

sealed trait BinaryLogicalOperator {}

object BinaryLogicalOperator {
  case object And extends BinaryLogicalOperator
  case object Or extends BinaryLogicalOperator
}