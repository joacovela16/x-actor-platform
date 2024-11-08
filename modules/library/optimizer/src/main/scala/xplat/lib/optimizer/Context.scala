package xplat.lib.optimizer

import breeze.linalg.DenseVector

final case class Context(store: Context.Store) {
  def get(id: String, index: Seq[Int]): Option[Context.Datum] = None
}

object Context {
  sealed trait Datum {}

  object Datum {
    final case class DString(value: String) extends Datum
    final case class DDouble(value: Double) extends Datum
  }

  sealed trait Data

  object Data {
    final case class Single(data: Datum) extends Data
    final case class Vector(data: DenseVector[Datum]) extends Data
    final case class Matrix2(data: DenseVector[DenseVector[Datum]]) extends Data
    final case class Matrix3(data: DenseVector[DenseVector[DenseVector[Datum]]]) extends Data
    final case class Matrix4(data: DenseVector[DenseVector[DenseVector[DenseVector[Datum]]]]) extends Data
  }


  final case class Store(data: Map[String, Data])
}
