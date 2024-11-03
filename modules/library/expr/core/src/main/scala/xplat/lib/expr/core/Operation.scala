package xplat.lib.expr.core


final case class Operation(name: String, literal: String)

object Operation {

  case object Boolean {
    val OR: Operation = Operation("or", "OR")
    val AND: Operation = Operation("and", "AND")
    val EQUALS: Operation = Operation("equals", "=")
  }

  implicit def fromString(value:String):Operation = Operation(value, value)
}
