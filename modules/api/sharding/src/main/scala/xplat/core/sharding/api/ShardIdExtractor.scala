package xplat.core.sharding.api

/** Maps an entity to its shard. Every node must use the same mapping, or the same entity could start in two places. */
trait ShardIdExtractor {
  def shardId(entityId: String): String
}

object ShardIdExtractor {

  /** Spreads entities over `numberOfShards` shards by the hash of their id. A good rule is ten times the maximum number of
    * nodes. Changing it later moves entities between shards, so it must be fixed for the life of the cluster.
    */
  def hashCode(numberOfShards: Int): ShardIdExtractor = {
    require(numberOfShards > 0, s"numberOfShards must be positive, got $numberOfShards")
    (entityId: String) => Math.floorMod(entityId.hashCode, numberOfShards).toString
  }
}
