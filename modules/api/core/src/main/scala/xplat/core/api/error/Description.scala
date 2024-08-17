package xplat.core.api.error

import scala.language.implicitConversions

final case class Description(value: Option[String])
object Description {
  implicit def mkFromStr(str: String): Description = Description(Option(str))
}
