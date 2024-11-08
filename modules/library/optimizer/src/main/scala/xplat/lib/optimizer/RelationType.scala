package xplat.lib.optimizer


sealed trait RelationType {}

object RelationType {
  case object GreatThan extends RelationType
  case object GreatOrEqualsThan extends RelationType
  case object LessOrEqualsThan extends RelationType
  case object LessThan extends RelationType
}