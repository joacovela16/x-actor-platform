package xplat.lib.expr.core

sealed trait Type {}

object Type {
  case object Boolean extends Type
  case object String extends Type
  case object Int extends Type
  case object Float extends Type
  case object Byte extends Type
  final case class Clazz(name: String, attributes: List[Field]) extends Type
  def Seq(tpe: Type): Type = Clazz("seq", List(Field("data", tpe)))
  def Tuple(items: Type*): Type = Clazz("tuple", items.zipWithIndex.map { case (d, i) => Field(i.toString, d) }.toList)
  def apply[T]:Type = ???
}
