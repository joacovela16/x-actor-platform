package xplat.lib.optimizer.model.dsl

sealed trait IsNumber[T] {
  def asDouble(v: T): Double
}

object IsNumber {

  implicit val isInt: IsNumber[Int] = new IsNumber[Int] {
    override def asDouble(v: Int): Double = v.toDouble
  }

  implicit val isBool: IsNumber[Boolean] = new IsNumber[Boolean] {
    override def asDouble(v: Boolean): Double = if (v) 1 else 0
  }

  implicit val isDouble: IsNumber[Double] = new IsNumber[Double] {
    override def asDouble(v: Double): Double = v
  }

}
