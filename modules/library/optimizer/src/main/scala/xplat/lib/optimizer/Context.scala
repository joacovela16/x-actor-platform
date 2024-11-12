package xplat.lib.optimizer

import breeze.linalg.DenseVector

final case class Context(store: Context.Store) {
  def get(id: String, index: Seq[Int]): Option[Datum] = None
}

object Context {

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
