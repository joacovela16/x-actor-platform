package xplat.core.sharding.api.error

final case class EntityTypeNotInitialized(name: String) extends RuntimeException(s"Entity type \"$name\" is not initialized on this node")
