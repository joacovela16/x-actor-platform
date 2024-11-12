package xplat.lib.optimizer

final case class Typed[Source, Target](value: Target)

object Typed {
  implicit val int: Typed[Int, NumberType] = Typed(NumberType.Integer)
  implicit val bool: Typed[Boolean, NumberType] = Typed(NumberType.Boolean)
  implicit val double: Typed[Double, NumberType] = Typed(NumberType.Double)

  implicit val validInt: Typed[Int, ValidType] = Typed(ValidType.Number)
  implicit val validDouble: Typed[Double, ValidType] = Typed(ValidType.Number)
  implicit val validBoolean: Typed[Boolean, ValidType] = Typed(ValidType.Number)
  implicit val validString: Typed[String, ValidType] = Typed(ValidType.String)

}
