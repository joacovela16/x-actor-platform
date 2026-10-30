package xplat.core.sharding.api

import xplat.core.coordination.LeaseUsage

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** @param role
  *   if set, entities only run on nodes with this role. Other nodes can still send to them.
  * @param passivateIdleAfter
  *   stops entities that received no message for this time. They start again on the next message.
  * @param rememberEntities
  *   restarts the entities that were running when their shard moves or its node crashes, instead of waiting for a message.
  * @param handOffTimeout
  *   time given to the entities of a shard to stop when the shard moves to another node.
  * @param rebalanceInterval
  *   how often the coordinator checks whether shards should move to balance the nodes.
  * @param retryInterval
  *   how often unanswered requests to the coordinator are retried, for example while it moves to another node.
  * @param lease
  *   if set, each shard holds the lease `<system>-shard-<type>-<shardId>` while it runs, and stops its entities if the
  *   lease is lost, so a shard never runs on two nodes. Needs a `LeaseProvider[F]`.
  */
final case class ShardingConfig(
    role: Option[String] = None,
    passivateIdleAfter: Option[FiniteDuration] = Some(2.minutes),
    rememberEntities: Boolean = false,
    handOffTimeout: FiniteDuration = 10.seconds,
    rebalanceInterval: FiniteDuration = 10.seconds,
    retryInterval: FiniteDuration = 2.seconds,
    lease: Option[LeaseUsage] = None
)
