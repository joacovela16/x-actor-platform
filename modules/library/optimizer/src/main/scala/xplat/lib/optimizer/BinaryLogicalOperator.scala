package xplat.lib.optimizer

sealed trait BinaryLogicalOperator {}

object BinaryLogicalOperator {
  case object And extends BinaryLogicalOperator
  case object Or extends BinaryLogicalOperator
}