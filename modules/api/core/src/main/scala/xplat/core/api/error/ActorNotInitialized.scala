package xplat.core.api.error

final case class ActorNotInitialized(name: String) extends RuntimeException(s"Actor \"$name\" not initialized")
