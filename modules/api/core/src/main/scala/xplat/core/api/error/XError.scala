package xplat.core.api.error

final case class XError(message: String) extends RuntimeException(message)
