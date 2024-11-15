package xplat.lib.optimizer.model

sealed trait ObjectiveKind

object ObjectiveKind {
  case object Min extends ObjectiveKind
  case object Max extends ObjectiveKind
}