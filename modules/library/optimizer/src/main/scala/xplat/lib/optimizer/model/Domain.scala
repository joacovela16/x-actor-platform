package xplat.lib.optimizer.model

sealed trait Domain[+T] {}

object Domain {
  case object Empty extends Domain[Nothing]
  final case class NonEmpty[T]() extends Domain[T]

}