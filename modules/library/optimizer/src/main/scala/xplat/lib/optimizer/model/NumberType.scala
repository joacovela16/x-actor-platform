package xplat.lib.optimizer.model

sealed trait NumberType {}

object NumberType {
   case object Integer extends NumberType
   case object Boolean extends NumberType
   case object Double extends NumberType
}