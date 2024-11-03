package xplat.lib.expr.core

sealed trait Type

object Type {
  trait Convert[T] extends (() => Type)

  object Convert {
    implicit val string: Convert[String] = () => Type.String
    implicit val int: Convert[Int] = () => Type.Int
    implicit val boolean: Convert[Boolean] = () => Type.Boolean
    implicit val float: Convert[Float] = () => Type.Float
    implicit val byte: Convert[Byte] = () => Type.Byte
    implicit def seq[T](implicit c: Convert[T]): Convert[Seq[T]] = () => Type.Seq(c())
  }

  case object Boolean extends Type
  case object String extends Type
  case object Int extends Type
  case object Float extends Type
  case object Byte extends Type
  final case class Clazz(name: String, attributes: List[Field]) extends Type
  def Seq(tpe: Type): Type = Clazz("seq", List(Field("data", tpe)))
  def Tuple(items: Type*): Type = Clazz("tuple", items.zipWithIndex.map { case (d, i) => Field(i.toString, d) }.toList)

  def apply[T](implicit c: Convert[T]): Type = c()
}
