package xplat.lib.optimizer

import scala.language.implicitConversions

sealed trait Datum {}

object Datum {
  final case class DString(value: String) extends Datum
  final case class DDouble(value: Double) extends Datum

  implicit def fromNumber(d: Double): Datum = DDouble(d)
  implicit def fromNumber(d: Int): Datum = DDouble(d)
  implicit def fromBoolean(d: Boolean): Datum = DDouble(if (d) 1 else 0)
  implicit def fromString(d: String): Datum = DString(d)
}
