package xplat.lib.optimizer

import breeze.linalg.DenseVector
import com.softwaremill.quicklens.ModifyPimp
import xplat.lib.optimizer.Context.Data

final case class Context(store: Context.Store) {
  private val data = store.data
  def get(id: String, index: Seq[Int]): Option[Datum] = None
  def get(id: String): Option[Datum] = data.get(id).collect { case Data.Single(data) => data }
  def getNumber(id: String): Option[Double] = get(id).collect { case Datum.DDouble(value) => value }
  def getStream(id: String): LazyList[Datum] = data.get(id).collect { case Data.Stream(data) => data }.getOrElse(LazyList.empty)
  def put(id: String, data: Context.Data): Context = this.modify(_.store.data).using(xs => xs + (id -> data))
}

object Context {
  def apply():Context = Context(Store(Map.empty))

  sealed trait Data

  object Data {
    final case class Stream(data: LazyList[Datum]) extends Data
    final case class Single(data: Datum) extends Data
    final case class Vector(data: DenseVector[Datum]) extends Data
    final case class Matrix2(data: DenseVector[DenseVector[Datum]]) extends Data
    final case class Matrix3(data: DenseVector[DenseVector[DenseVector[Datum]]]) extends Data
    final case class Matrix4(data: DenseVector[DenseVector[DenseVector[DenseVector[Datum]]]]) extends Data
  }

  final case class Store(data: Map[String, Data])
}
