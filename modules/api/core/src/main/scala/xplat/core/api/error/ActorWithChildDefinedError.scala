package xplat.core.api.error

final case class ActorWithChildDefinedError(name: String, childName: String) extends RuntimeException(s"Actor \"$name\" has child stop \"$childName\"")
