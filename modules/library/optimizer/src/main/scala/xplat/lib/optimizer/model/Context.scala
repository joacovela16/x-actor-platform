package xplat.lib.optimizer.model

import com.softwaremill.quicklens.ModifyPimp
import xplat.lib.optimizer.model.Context.Data

final case class Context(
    data: Map[String, Data],
    args: Map[Int, Datum]
) {
  def get(id: String, index: Seq[Int]): Option[Datum] = None
  def get(id: String): Option[Datum] = data.get(id).collect { case Data.Single(data) => data }
  def getNumber(id: String): Option[Double] = get(id).collect { case Datum.DDouble(value) => value }
  def withArg(i: Int, d: Datum): Context = this.modify(_.args).using(xs => xs + (i -> d))

  def getSize(id: String): Long = {
    data.get(id) match {
      case Some(d) =>
        d match {
          case Data.Single(_)     => 1
          case Data.Stream(data)  => data.size
          case Data.Matrix(data)  => data.size
          case Data.Matrix2(data) => data.size
          case Data.Matrix3(data) => data.size
          case Data.Matrix4(data) => data.size
        }
      case None => 0L
    }
  }

  def put[D](s: SSet[D], head: D, tail: D*)(implicit b: Context.DataBuilder.Aux[SSet[D], Seq[D]]): Context = this.modify(_.data).using(xs => xs + (s.id -> b.build(head +: tail)))
  def put[T, D](s: Param[T, D], head: (D, T), tail: (D, T)*)(implicit b: Context.DataBuilder.Aux[Param[T, D], Seq[(D, T)]]): Context = this.modify(_.data).using(xs => xs + (s.id -> b.build(head +: tail)))
//  def put[T, D](s: Param[T, D])(head: (D, T), tail: (D, T)*)(implicit b: Context.DataBuilder.Aux[Param[T, D], Seq[(D, T)]]): Context = this.modify(_.store.data).using(xs => xs + (s.id -> b.build(head +: tail)))
  def put[D](s: Param[D, Unit], v: D)(implicit b: Context.DataBuilder.Aux[Param[D, Unit], D]): Context = this.modify(_.data).using(xs => xs + (s.id -> b.build(v)))
  def put(id: String, data: Data): Context = this.modify(_.data).using(xs => xs + (id -> data))

  def getStream(source: Seq[String]): LazyList[Context] = {
    val base = source.flatMap { id =>
      data.get(id).collect { case Data.Stream(data) => data }
    }.zipWithIndex

    def loop(xs: Seq[(LazyList[Datum], Int)], base: Context): LazyList[Context] = {
      xs match {
        case Nil =>
          LazyList(base)
        case (head, index) +: tail =>
          head.flatMap { d =>
            loop(tail, base.withArg(index, d))
          }
      }
    }

    loop(base, this)
  }

}

object Context {
  private val empty = Context(Map.empty, Map.empty)

  def apply(): Context = {
    empty
  }

  sealed trait Data

  object Data {
    final case class Single(data: Datum) extends Data
    final case class Stream(data: LazyList[Datum]) extends Data
    final case class Matrix(data: LazyList[(Datum, Datum)]) extends Data
    final case class Matrix2(data: LazyList[((Datum, Datum), Datum)]) extends Data
    final case class Matrix3(data: LazyList[((Datum, Datum, Datum), Datum)]) extends Data
    final case class Matrix4(data: LazyList[((Datum, Datum, Datum, Datum), Datum)]) extends Data
  }

  final case class Store(data: Map[String, Data])

  sealed trait DataBuilder[Domain] {
    type Apply
    def build(data: Apply): Data
  }

  object DataBuilder extends DataBuilderLP {
    type Aux[Domain, Apply0] = DataBuilder[Domain] { type Apply = Apply0 }

    implicit def singleParam[T](implicit c: Datum.Codec[T]): DataBuilder.Aux[Param[T, Unit], T] = new DataBuilder[Param[T, Unit]] {
      override type Apply = T

      override def build(data: T): Data = {
        Data.Single(c.asDatum(data))
      }

    }

    implicit def mkMatrix2[T, A, B](implicit a: Datum.Codec[A], b: Datum.Codec[B], c: Datum.Codec[T]): DataBuilder.Aux[Param[T, (A, B)], Seq[((A, B), T)]] = new DataBuilder[Param[T, (A, B)]] {
      type Apply = Seq[((A, B), T)]

      override def build(data: Apply): Data = {
        Data.Matrix2(
          data.to(LazyList).map(x => ((a.asDatum(x._1._1), b.asDatum(x._1._2)), c.asDatum(x._2)))
        )
      }

    }
    /*
    implicit def mkMatrix2[A, B]: DataBuilder.Aux[(SSet[A], SSet[B]), ((Datum, Datum), Datum)] = new DataBuilder[(SSet[A], SSet[B]), ((Datum, Datum), Datum)] {
      type Apply = ((Datum, Datum), Datum)

      override def build(data: Seq[Apply]): Data = {
        Data.Matrix2(data.to(LazyList))
      }

    }*/

  }

  trait DataBuilderLP {

    implicit def mkVector4Param[D, T](implicit c: Datum.Codec[T], d: Datum.Codec[D]): DataBuilder.Aux[Param[T, D], Seq[(D, T)]] = new DataBuilder[Param[T, D]] {
      type Apply = Seq[(D, T)]

      override def build(data: Apply): Data = {
        Data.Matrix(data.to(LazyList).map(x => d.asDatum(x._1) -> c.asDatum(x._2)))
      }

    }

    implicit def mkStream4Set[D](implicit c: Datum.Codec[D]): DataBuilder.Aux[SSet[D], Seq[D]] = new DataBuilder[SSet[D]] {
      type Apply = Seq[D]

      override def build(data: Apply): Data = {
        Data.Stream(data.to(LazyList).map(c.asDatum))
      }

    }

  }

}
