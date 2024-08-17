package xplat.core.persistence.api

sealed trait Effect[+Event]

object Effect {

  case object Stash extends Effect[Nothing]

  case object Unstash extends Effect[Nothing]

  case object Ignore extends Effect[Nothing]

  final case class Persist[Event](value: Event) extends Effect[Event]

  final case object Compress extends Effect[Nothing]
}
