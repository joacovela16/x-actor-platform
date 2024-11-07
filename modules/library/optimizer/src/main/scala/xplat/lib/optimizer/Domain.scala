package xplat.lib.optimizer


sealed trait Domain[+T] {}

object Domain {
  case object Empty extends Domain[Nothing]
  final case class NonEmpty[T]() extends Domain[T]

}