package xplat.core.sharding.api

/** Identifies a kind of sharded entity and the type of messages it accepts. The name must be unique in the cluster. */
final case class EntityTypeKey[T](name: String)
