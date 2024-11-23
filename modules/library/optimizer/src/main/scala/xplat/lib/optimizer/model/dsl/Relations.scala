package xplat.lib.optimizer.model.dsl

sealed trait Relations

object Relations {
  case object Equals extends Relations
  case object Less extends Relations
  case object LessEqual extends Relations
  case object Great extends Relations
  case object GreatEqual extends Relations
}
