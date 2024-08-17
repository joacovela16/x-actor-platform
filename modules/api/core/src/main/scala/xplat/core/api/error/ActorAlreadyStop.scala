package xplat.core.api.error

final case class ActorAlreadyStop(name: String) extends RuntimeException(s"Actor \"$name\" already stop")
