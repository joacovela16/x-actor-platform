package xplat.lib.optimizer.model

import scala.language.implicitConversions

sealed trait Datum {

  def asNumber: Option[Double] = this
  /** EndMarker */
  match {
    case Datum.DDouble(value) => Option(value)
    case _                    => None
  }

  def asString: Option[String] = this match {
    case Datum.DString(value) => Option(value)
    case _                    => None
  }

}

object Datum {
  final case class DString(value: String) extends Datum{
    override def toString: String = value
  }
  final case class DDouble(value: Double) extends Datum{
    override def toString: String = value.toString
  }

  implicit def fromNumber(d: Double): Datum = DDouble(d)
  implicit def fromNumber(d: Int): Datum = DDouble(d)
  implicit def fromBoolean(d: Boolean): Datum = DDouble(if (d) 1 else 0)
  implicit def fromString(d: String): Datum = DString(d)

  trait Codec[T] {
    def asDatum(value: T): Datum
  }

  object Codec {
    def apply[T](implicit c: Codec[T]): Codec[T] = c
    implicit def mkDouble: Codec[Double] = v => v
    implicit def mkInt: Codec[Int] = v => v
    implicit def mkBoolean: Codec[Boolean] = v => v
    implicit def mkString: Codec[String] = v => v
  }

}
