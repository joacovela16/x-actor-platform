package xplat.lib.optimizer

sealed trait IsValidVariable[T] {
  def asDouble(v: T): Double
}

object IsValidVariable {

  implicit val isInt: IsValidVariable[Int] = new IsValidVariable[Int] {
    override def asDouble(v: Int): Double = v.toDouble
  }

  implicit val isBool: IsValidVariable[Boolean] = new IsValidVariable[Boolean] {
    override def asDouble(v: Boolean): Double = if (v) 1 else 0
  }

  implicit val isDouble: IsValidVariable[Double] = new IsValidVariable[Double] {
    override def asDouble(v: Double): Double = v
  }

}
