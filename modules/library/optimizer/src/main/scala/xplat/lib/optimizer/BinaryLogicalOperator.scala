package xplat.lib.optimizer

sealed trait BinaryLogicalOperator {}

object BinaryLogicalOperator {
  case object LessThan extends BinaryLogicalOperator
  case object LessEqualThan extends BinaryLogicalOperator
  case object GreatThan extends BinaryLogicalOperator
  case object GreatEqualThan extends BinaryLogicalOperator
  case object Equals extends BinaryLogicalOperator
}