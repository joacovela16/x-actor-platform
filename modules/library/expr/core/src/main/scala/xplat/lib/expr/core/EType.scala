package xplat.lib.expr.core

sealed trait EType

object EType {
  trait Convert[T] extends (() => EType)

  object Convert {
    implicit val string: Convert[String] = () => EType.String
    implicit val int: Convert[Int] = () => EType.Int
    implicit val boolean: Convert[Boolean] = () => EType.Boolean
    implicit val float: Convert[Float] = () => EType.Float
    implicit val byte: Convert[Byte] = () => EType.Byte
    implicit def seq[T](implicit c: Convert[T]): Convert[Seq[T]] = () => EType.Seq(c())
  }

  case object Boolean extends EType
  case object String extends EType
  case object Int extends EType
  case object Float extends EType
  case object Byte extends EType
  final case class Clazz(name: String, attributes: List[Field]) extends EType
  def Seq(tpe: EType): EType = Clazz("seq", List(Field("data", tpe)))
  def Tuple(items: EType*): EType = Clazz("tuple", items.zipWithIndex.map { case (d, i) => Field(i.toString, d) }.toList)

  def apply[T](implicit c: Convert[T]): EType = c()
}
