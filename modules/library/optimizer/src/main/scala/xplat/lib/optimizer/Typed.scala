package xplat.lib.optimizer

final case class Typed[Source, Target](value: Target)

object Typed{
  implicit val int:Typed[Int, Kind] = Typed(Kind.Integer)
  implicit val bool:Typed[Boolean, Kind] = Typed(Kind.Boolean)
  implicit val double:Typed[Double, Kind] = Typed(Kind.Double)
}