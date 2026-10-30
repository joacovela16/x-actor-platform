package xplat.core.sharding.api

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** @param role
  *   if set, entities only run on nodes with this role. Other nodes can still send to them.
  * @param passivateIdleAfter
  *   stops entities that received no message for this time. They start again on the next message.
  * @param rememberEntities
  *   restarts the entities that were running when their shard moves or its node crashes, instead of waiting for a message.
  * @param handOffTimeout
  *   time given to the entities of a shard to stop when the shard moves to another node.
  */
final case class ShardingConfig(
    role: Option[String] = None,
    passivateIdleAfter: Option[FiniteDuration] = Some(2.minutes),
    rememberEntities: Boolean = false,
    handOffTimeout: FiniteDuration = 10.seconds
)
