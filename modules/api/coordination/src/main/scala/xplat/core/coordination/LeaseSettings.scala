package xplat.core.coordination

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** @param name
  *   identifies the lease in the external store. Everyone competing for the same resource uses the same name.
  * @param owner
  *   identifies the holder, usually the node address. Must be unique among competitors.
  * @param timeToLive
  *   if the owner stops renewing (it crashed or lost the connection), the lease is freed after this time.
  * @param heartbeatInterval
  *   how often the owner renews the lease. Must be well below `timeToLive`, so a slow renewal does not lose it.
  * @param operationTimeout
  *   maximum time for one call to the external store. A call that times out counts as failed.
  */
final case class LeaseSettings(
    name: String,
    owner: String,
    timeToLive: FiniteDuration = 120.seconds,
    heartbeatInterval: FiniteDuration = 12.seconds,
    operationTimeout: FiniteDuration = 5.seconds
) {
  require(heartbeatInterval < timeToLive, s"heartbeatInterval ($heartbeatInterval) must be shorter than timeToLive ($timeToLive)")
}

/** How a singleton or a shard uses a lease, on top of the cluster rules, so it never runs on two nodes at once.
  *
  * The lease name is derived from what it protects (e.g. the singleton name), and the owner is the node address.
  *
  * @param retryInterval
  *   how long to wait before trying again when the lease is taken by someone else.
  */
final case class LeaseUsage(
    retryInterval: FiniteDuration = 5.seconds,
    timeToLive: FiniteDuration = 120.seconds,
    heartbeatInterval: FiniteDuration = 12.seconds
)
