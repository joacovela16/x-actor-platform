package xplat.lib.optimizer

sealed trait BinaryOperator {

}

object BinaryOperator {
  case object Equals extends BinaryOperator
  case object Less extends BinaryOperator
  case object LessEqual extends BinaryOperator
  case object Great extends BinaryOperator
  case object GreatEqual extends BinaryOperator
  case object Plus extends BinaryOperator
  case object Minus extends BinaryOperator
  case object Times extends BinaryOperator
  case object Divide extends BinaryOperator
}
