package xplat.lib.optimizer

sealed trait BinaryOperator {

}

object BinaryOperator {
  case object Plus extends BinaryOperator
  case object Minus extends BinaryOperator
  case object Times extends BinaryOperator
  case object Divide extends BinaryOperator
}
