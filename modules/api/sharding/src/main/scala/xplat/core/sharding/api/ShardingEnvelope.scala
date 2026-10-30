package xplat.core.sharding.api

/** A message addressed to an entity, for sending through the shard region instead of an [[EntityRef]]. */
final case class ShardingEnvelope[T](entityId: String, message: T)
