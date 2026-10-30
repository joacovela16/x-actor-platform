package xplat.core.io.sharding

import cats.Applicative
import xplat.core.cluster.api.Address
import xplat.core.sharding.api.ShardAllocationStrategy

/** Default strategy: a new shard goes to the node with the fewest shards, and shards move from the fullest to the
  * emptiest node while they differ by `rebalanceThreshold` or more.
  *
  * @param maxSimultaneousRebalance
  *   shards that can be moving at the same time; limits how much work stops during a rebalance.
  */
final class LeastShardAllocationStrategy[F[_]](rebalanceThreshold: Int = 2, maxSimultaneousRebalance: Int = 3)(implicit F: Applicative[F]) extends ShardAllocationStrategy[F] {
  require(rebalanceThreshold >= 2, "rebalanceThreshold must be at least 2, or shards would move back and forth")

  def allocate(requester: Address, shardId: String, current: Map[Address, Set[String]]): F[Address] = {
    val fewest = current.values.map(_.size).min
    val candidates = current.collect { case (a, shards) if shards.size == fewest => a }
    // Prefer the requester on a tie: its messages then stay local.
    F.pure(if (candidates.exists(_ == requester)) requester else candidates.min)
  }

  def rebalance(current: Map[Address, Set[String]], inProgress: Set[String]): F[Set[String]] = {
    val room = maxSimultaneousRebalance - inProgress.size
    if (current.size < 2 || room <= 0) F.pure(Set.empty)
    else {
      val (fullest, shards) = current.maxBy { case (a, s) => (s.size, a) }(Ordering.Tuple2(Ordering.Int, Ordering[Address].reverse))
      val emptiest = current.values.map(_.size).min
      val difference = shards.size - emptiest
      if (difference < rebalanceThreshold) F.pure(Set.empty)
      else F.pure((shards -- inProgress).toList.sorted.take(math.min(difference / 2, room)).toSet)
    }
  }
}
