package xplat.core.sharding.api

/** Shards of one entity type hosted by the local node, with the ids of their running entities. */
final case class ShardRegionState(shards: Map[String, Set[String]]) {
  def entityIds: Set[String] = shards.values.flatten.toSet
}
