package xplat.core.io.cluster.internal

import xplat.core.cluster.api.{FailureDetectorConfig, UniqueAddress}

/** Intervals between the heartbeats of one member, with running sums for mean and variance. */
final case class HeartbeatHistory(maxSampleSize: Int, intervals: Vector[Long], sum: Long, squaredSum: Long) {

  def mean: Double = sum.toDouble / intervals.size

  def variance: Double = squaredSum.toDouble / intervals.size - mean * mean

  def stdDeviation: Double = math.sqrt(math.max(variance, 0.0))

  def :+(interval: Long): HeartbeatHistory =
    if (intervals.size < maxSampleSize) HeartbeatHistory(maxSampleSize, intervals :+ interval, sum + interval, squaredSum + interval * interval)
    else {
      val dropped = intervals.head
      HeartbeatHistory(maxSampleSize, intervals.tail :+ interval, sum - dropped + interval, squaredSum - dropped * dropped + interval * interval)
    }
}

/** Phi accrual failure detector state for one member. Times are in milliseconds. */
final case class PhiAccrual(config: FailureDetectorConfig, history: HeartbeatHistory, lastHeartbeat: Long) {

  def heartbeat(at: Long): PhiAccrual = {
    val interval = at - lastHeartbeat
    copy(history = if (interval > 0) history :+ interval else history, lastHeartbeat = at)
  }

  def phi(at: Long): Double = {
    val mean = history.mean + config.acceptableHeartbeatPause.toMillis
    val deviation = math.max(history.stdDeviation, config.minStdDeviation.toMillis.toDouble)
    PhiAccrual.phi((at - lastHeartbeat).toDouble, mean, deviation)
  }

  def isAvailable(at: Long): Boolean = phi(at) < config.threshold
}

object PhiAccrual {

  /** Starts monitoring as if a heartbeat had just arrived, assuming the expected interval with some variation. */
  def start(config: FailureDetectorConfig, expectedInterval: Long, at: Long): PhiAccrual = {
    val deviation = expectedInterval / 4
    val history = HeartbeatHistory(config.maxSampleSize, Vector.empty, 0L, 0L) :+ (expectedInterval - deviation) :+ (expectedInterval + deviation)
    PhiAccrual(config, history, at)
  }

  /** -log10 of the probability that a heartbeat arrives later than `elapsed`, using a logistic approximation of the
    * normal distribution (as Akka does).
    */
  def phi(elapsed: Double, mean: Double, stdDeviation: Double): Double = {
    val y = (elapsed - mean) / stdDeviation
    val e = math.exp(-y * (1.5976 + 0.070566 * y * y))
    if (elapsed > mean) -math.log10(e / (1.0 + e)) else -math.log10(1.0 - 1.0 / (1.0 + e))
  }
}

/** Who watches whom. Members sit on a ring sorted by address, and each member is watched by the next `monitoredBy` ones,
  * so every node exchanges heartbeats with a fixed number of neighbours whatever the size of the cluster.
  */
object HeartbeatRing {

  /** Members that watch `node`: it sends them heartbeats. */
  def receivers(node: UniqueAddress, members: Set[UniqueAddress], monitoredBy: Int): Set[UniqueAddress] = {
    val ring = (members + node).toVector.sorted
    val i = ring.indexOf(node)
    (1 to math.min(monitoredBy, ring.size - 1)).map(k => ring((i + k) % ring.size)).toSet
  }

  /** Members that `node` watches: those whose receivers include it. */
  def monitored(node: UniqueAddress, members: Set[UniqueAddress], monitoredBy: Int): Set[UniqueAddress] = {
    val ring = (members + node).toVector.sorted
    val i = ring.indexOf(node)
    (1 to math.min(monitoredBy, ring.size - 1)).map(k => ring(((i - k) % ring.size + ring.size) % ring.size)).toSet
  }
}
