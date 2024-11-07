package xplat.lib.optimizer

sealed trait SSet[T] {
  def id:String
}

object SSet {
  final case class Open[T](id:String) extends SSet[T]
  final case class Literal[T](id:String, values: Seq[T]) extends SSet[T]
  final case class Range[T](id:String, from: T, to: T) extends SSet[T]
  final case class Union[T](id:String, data: Seq[SSet[T]]) extends SSet[T]
}
