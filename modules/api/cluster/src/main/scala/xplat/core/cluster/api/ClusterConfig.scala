package xplat.core.cluster.api

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** @param host
  *   interface this node binds to and advertises.
  * @param port
  *   port this node binds to; 0 picks a free one.
  * @param seedNodes
  *   nodes contacted to join. Empty means the node waits for an explicit [[xplat.core.cluster.Cluster.join]].
  * @param roles
  *   roles of this node, used to place work (e.g. sharding only on "backend" nodes).
  * @param gossipInterval
  *   how often this node shares its view of the cluster with the other members.
  * @param retryInterval
  *   how often join requests and other unanswered internal requests are retried.
  * @param heartbeatInterval
  *   how often the failure detector checks other members.
  * @param unreachableAfter
  *   silence after which a member is reported as unreachable.
  * @param splitBrainResolver
  *   downs members automatically when the cluster splits. `None` leaves downing to the user (see
  *   [[xplat.core.cluster.Cluster.down]]); until then, unreachable members block membership changes.
  */
final case class ClusterConfig(
    host: String = "127.0.0.1",
    port: Int = 2552,
    seedNodes: List[Address] = Nil,
    roles: Set[String] = Set.empty,
    gossipInterval: FiniteDuration = 1.second,
    retryInterval: FiniteDuration = 1.second,
    heartbeatInterval: FiniteDuration = 1.second,
    unreachableAfter: FiniteDuration = 5.seconds,
    splitBrainResolver: Option[SplitBrainResolverConfig] = None
)
