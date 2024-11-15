package xplat.lib.optimizer.model

trait Combine[A, B] {
  type Out
}

object Combine {
  type Aux[A, B, Out0] = Combine[A, B] { type Out = Out0 }
  implicit val i2d : Combine.Aux[Int, Double, Double] = null
  implicit val d2i : Combine.Aux[Double, Int, Double] = null
  implicit val i2i : Combine.Aux[Int, Int, Int] = null
  implicit val d2d: Combine.Aux[Double, Double, Double] = null
}
