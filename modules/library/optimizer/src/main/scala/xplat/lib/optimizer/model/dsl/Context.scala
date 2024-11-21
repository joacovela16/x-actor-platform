package xplat.lib.optimizer.model.dsl

import com.softwaremill.quicklens.ModifyPimp

final case class Context(
    input: Map[String, Context.Data],
    args: Map[String, Datum],
    variables: Map[String, Double]
) {
  def get(id: String, index: Seq[Int]): Option[Datum] = None
  def get(id: String): Option[Datum] = input.get(id).collect { case Context.Data.Param1(data) => data }
  def withArg(id: String, d: Datum): Context = this.modify(_.args).using(xs => xs + (id -> d))

  def put[D](s: SSet[D], head: D, tail: D*)(implicit b: Context.DataBuilder.Aux[SSet[D], Seq[D]]): Context = this.modify(_.input).using(xs => xs + (s.id -> b.build(head +: tail)))
  def put[T, D](s: Param[T, D], head: (D, T), tail: (D, T)*)(implicit b: Context.DataBuilder.Aux[Param[T, D], Seq[(D, T)]]): Context = this.modify(_.input).using(xs => xs + (s.id -> b.build(head +: tail)))
  def put[D](s: Param[D, Unit], v: D)(implicit b: Context.DataBuilder.Aux[Param[D, Unit], D]): Context = this.modify(_.input).using(xs => xs + (s.id -> b.build(v)))
  def put(id: String, data: Context.Data): Context = this.modify(_.input).using(xs => xs + (id -> data))
  def withoutArgs: Context = this.modify(_.args).setTo(Map.empty)

  def getStream(source: Seq[String]): LazyList[Context] = {
    val base: Seq[(String, LazyList[Datum])] = source.flatMap { id =>
      input.get(id).collect { case Context.Data.Set(data) => data }.map(xs => id -> xs)
    }

    def loop(xs: Seq[(String, LazyList[Datum])], base: Context): LazyList[Context] = {
      xs match {
        case Nil =>
          LazyList(base)
        case (id, head) +: tail =>
          head.flatMap { d =>
            loop(tail, base.withArg(id, d))
          }
      }
    }

    loop(base, this)
  }

}

object Context {
  private val empty = Context(Map.empty, Map.empty, Map.empty)

  def apply(): Context = {
    empty
  }

  sealed trait Data

  object Data {
    final case class Set(data: LazyList[Datum]) extends Data
    final case class Param1(data: Double) extends Data
    final case class Param2(data: Map[String, Double]) extends Data
//    final case class Matrix2(data: LazyList[((Datum, Datum), Datum)]) extends Data
//    final case class Matrix3(data: LazyList[((Datum, Datum, Datum), Datum)]) extends Data
//    final case class Matrix4(data: LazyList[((Datum, Datum, Datum, Datum), Datum)]) extends Data
  }

  final case class Variable(id: String, reference: Seq[Int])

  sealed trait DataBuilder[Domain] {
    type Apply
    def build(data: Apply): Data
  }

  object DataBuilder extends DataBuilderLP {
    type Aux[Domain, Apply0] = DataBuilder[Domain] { type Apply = Apply0 }

    implicit def singleParam[T](implicit n: IsNumber[T]): DataBuilder.Aux[Param[T, Unit], T] = new DataBuilder[Param[T, Unit]] {
      override type Apply = T

      override def build(data: T): Data = {
        Data.Param1(n.asDouble(data))
      }

    }

    implicit def mkMatrix2[A, B, T](implicit n: IsNumber[T]): DataBuilder.Aux[Param[T, (A, B)], Seq[((A, B), T)]] = new DataBuilder[Param[T, (A, B)]] {
      type Apply = Seq[((A, B), T)]

      override def build(data: Apply): Data = {
        Data.Param2(
          data.map(x => s"${x._1._1}${x._1._2}" -> n.asDouble(x._2)).toMap
        )
      }

    }

  }

  trait DataBuilderLP {

    implicit def mkVector4Param[D, T](implicit n: IsNumber[T]): DataBuilder.Aux[Param[T, D], Seq[(D, T)]] = new DataBuilder[Param[T, D]] {
      type Apply = Seq[(D, T)]

      override def build(data: Apply): Data = {
        Data.Param2(data.map(x => x._1.toString -> n.asDouble(x._2)).toMap)
      }

    }

    implicit def mkStream4Set[D](implicit c: Datum.Codec[D]): DataBuilder.Aux[SSet[D], Seq[D]] = new DataBuilder[SSet[D]] {
      type Apply = Seq[D]

      override def build(data: Apply): Data = {
        Data.Set(data.to(LazyList).map(c.asDatum))
      }

    }

  }

}
