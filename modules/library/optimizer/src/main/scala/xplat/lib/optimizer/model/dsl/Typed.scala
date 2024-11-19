package xplat.lib.optimizer.model.dsl

final case class Typed[Source, Target](value: Target)

object Typed {
  implicit val int: Typed[Int, NumberType] = Typed(NumberType.Integer)
  implicit val bool: Typed[Boolean, NumberType] = Typed(NumberType.Boolean)
  implicit val double: Typed[Double, NumberType] = Typed(NumberType.Double)

  implicit val validInt: Typed[Int, DomainType] = Typed(DomainType.Number)
//  implicit val validDouble: Typed[Double, DomainType] = Typed(DomainType.Number)
  implicit val validBoolean: Typed[Boolean, DomainType] = Typed(DomainType.Number)
  implicit val validString: Typed[String, DomainType] = Typed(DomainType.String)

}
