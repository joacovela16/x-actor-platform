package xplat.lib.optimizer

sealed trait BinaryRelation {

}

object BinaryRelation {
  case object Equals extends BinaryRelation
  case object Less extends BinaryRelation
  case object LessEqual extends BinaryRelation
  case object Great extends BinaryRelation
  case object GreatEqual extends BinaryRelation
}
