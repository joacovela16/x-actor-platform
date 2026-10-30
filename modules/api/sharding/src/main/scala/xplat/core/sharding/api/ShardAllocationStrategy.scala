package xplat.core.sharding.api

import xplat.core.cluster.api.Address

/** Decides on which node each shard lives.
  *
  * `current` maps every node that hosts shards of this entity type to the shard ids it holds.
  */
trait ShardAllocationStrategy[F[_]] {

  /** Picks the node for a shard that has no home yet. `requester` is the node that received the first message for it. */
  def allocate(requester: Address, shardId: String, current: Map[Address, Set[String]]): F[Address]

  /** Runs periodically and returns the shards to move. They are handed off and allocated again with [[allocate]].
    * `inProgress` holds the shards still being moved.
    */
  def rebalance(current: Map[Address, Set[String]], inProgress: Set[String]): F[Set[String]]
}
