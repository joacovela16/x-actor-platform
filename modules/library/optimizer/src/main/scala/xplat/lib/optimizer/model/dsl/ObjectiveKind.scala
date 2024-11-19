package xplat.lib.optimizer.model.dsl

sealed trait ObjectiveKind

object ObjectiveKind {
  case object Min extends ObjectiveKind
  case object Max extends ObjectiveKind
}