package xplat.lib.optimizer

sealed trait IsValidVariable[T]{
  def asDouble:Double
}

object IsValidVariable {
  implicit val isInt:IsValidVariable[Int] = ???
  implicit val isBool:IsValidVariable[Boolean] = ???
  implicit val isDouble:IsValidVariable[Double] = ???
}
