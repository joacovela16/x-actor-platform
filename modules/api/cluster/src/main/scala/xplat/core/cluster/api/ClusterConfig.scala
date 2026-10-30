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
  *   how often this node shares its view of the cluster.
  * @param gossipFanout
  *   members contacted on each gossip round. Those that have not seen the latest view go first, so a change reaches
  *   everyone in a few rounds without every node talking to every other.
  * @param retryInterval
  *   how often join requests and other unanswered internal requests are retried.
  * @param heartbeatInterval
  *   how often a node sends heartbeats to the members that watch it.
  * @param failureDetector
  *   when a silent member counts as unreachable.
  * @param pruneTombstonesAfter
  *   how long removed members are remembered, so stale messages cannot bring them back. After that they are forgotten,
  *   which keeps the gossip small in clusters where nodes come and go.
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
    gossipFanout: Int = 3,
    retryInterval: FiniteDuration = 1.second,
    heartbeatInterval: FiniteDuration = 1.second,
    failureDetector: FailureDetectorConfig = FailureDetectorConfig(),
    pruneTombstonesAfter: FiniteDuration = 24.hours,
    splitBrainResolver: Option[SplitBrainResolverConfig] = None,
    security: TransportSecurity = TransportSecurity()
) {
  require(gossipFanout > 0, "gossipFanout must be positive")
}

/** Phi accrual failure detector, as in Akka and Cassandra.
  *
  * Instead of a fixed timeout, it learns the usual interval between heartbeats of each member and how much it varies.
  * `phi` measures how unlikely the current silence is given that history; the member is unreachable when it goes over
  * `threshold`. A network with jittery latency then needs a longer silence than a steady one.
  *
  * @param threshold
  *   higher means fewer false alarms but slower detection. 8 suits most networks; 12 tolerates very irregular ones.
  * @param acceptableHeartbeatPause
  *   silence tolerated on top of the usual interval, e.g. for garbage collection pauses.
  * @param minStdDeviation
  *   lower bound for the variation, so very regular heartbeats do not make the detector too eager.
  * @param maxSampleSize
  *   intervals kept per member.
  * @param monitoredBy
  *   how many members watch each member. Each node only exchanges heartbeats with a few neighbours on a ring, so the
  *   traffic grows linearly with the cluster.
  */
final case class FailureDetectorConfig(
    threshold: Double = 8.0,
    acceptableHeartbeatPause: FiniteDuration = 3.seconds,
    minStdDeviation: FiniteDuration = 100.millis,
    maxSampleSize: Int = 1000,
    monitoredBy: Int = 5
) {
  require(threshold > 0, "threshold must be positive")
  require(maxSampleSize > 0, "maxSampleSize must be positive")
  require(monitoredBy > 0, "monitoredBy must be positive")
}

/** Protection of the traffic between nodes.
  *
  * @param tls
  *   encrypts the connections and checks the certificate of the other side (mutual TLS). Only used by the TCP transport.
  * @param sharedSecret
  *   every connection must prove it knows this secret before any message is accepted. The secret itself never travels:
  *   nodes answer a random challenge with an HMAC of it. Use it together with TLS, which also hides the messages.
  */
final case class TransportSecurity(tls: Option[TlsConfig] = None, sharedSecret: Option[String] = None)

/** Key and trust stores in PKCS12 or JKS format. Every node presents its certificate and only accepts certificates
  * signed by an authority in the trust store.
  */
final case class TlsConfig(
    keyStore: java.nio.file.Path,
    keyStorePassword: String,
    trustStore: java.nio.file.Path,
    trustStorePassword: String,
    keyPassword: Option[String] = None
)
