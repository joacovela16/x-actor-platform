package xplat.core.io.cluster

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.cluster.api.{Address, FailureDetectorConfig, UniqueAddress}
import xplat.core.io.cluster.internal.{HeartbeatHistory, HeartbeatRing, PhiAccrual}

import scala.concurrent.duration._

class FailureDetectionTest extends AnyFreeSpec with Matchers {

  private val config = FailureDetectorConfig(threshold = 8.0, acceptableHeartbeatPause = 0.millis, minStdDeviation = 10.millis)

  /** Detector that got heartbeats every `interval` ms until time `until`. */
  private def regular(interval: Long, until: Long): PhiAccrual =
    (interval to until by interval).foldLeft(PhiAccrual.start(config, interval, 0L))(_.heartbeat(_))

  "phi accrual" - {

    "a member that keeps sending heartbeats is available" in {
      val d = regular(100, 10000)
      d.isAvailable(10050) shouldBe true
    }

    "phi grows with the silence" in {
      val d = regular(100, 10000)
      d.phi(10100) should be < d.phi(10200)
      d.phi(10200) should be < d.phi(10400)
    }

    "a long silence makes the member unreachable" in {
      regular(100, 10000).isAvailable(11000) shouldBe false
    }

    "irregular heartbeats need a longer silence than regular ones" in {
      val jittery = (1 to 100).foldLeft(PhiAccrual.start(config, 100, 0L)) { (d, i) => d.heartbeat(d.lastHeartbeat + (if (i % 2 == 0) 40 else 160)) }
      val steady = regular(100, jittery.lastHeartbeat)
      val at = jittery.lastHeartbeat + 250
      jittery.phi(at) should be < steady.phi(at)
    }

    "the acceptable pause delays detection" in {
      val tolerant = (100L to 10000L by 100L).foldLeft(PhiAccrual.start(config.copy(acceptableHeartbeatPause = 2.seconds), 100, 0L))(_.heartbeat(_))
      (regular(100, 10000).isAvailable(11000), tolerant.isAvailable(11000)) shouldBe (false, true)
    }

    "the history keeps only the last samples" in {
      val h = (1 to 10).foldLeft(HeartbeatHistory(3, Vector.empty, 0L, 0L))((h, i) => h :+ i.toLong)
      (h.intervals, h.mean) shouldBe (Vector(8L, 9L, 10L), 9.0)
    }
  }

  "heartbeat ring" - {
    val nodes = (1 to 10).map(n => UniqueAddress(Address("sys", "h", n), n.toLong)).toSet

    "each member is watched by the next ones on the ring" in {
      val first = nodes.minBy(_.address.port)
      HeartbeatRing.receivers(first, nodes - first, 3).map(_.address.port) shouldBe Set(2, 3, 4)
    }

    "watching is symmetric with receiving" in {
      for (a <- nodes; b <- nodes if a != b)
        HeartbeatRing.receivers(a, nodes - a, 3)(b) shouldBe HeartbeatRing.monitored(b, nodes - b, 3)(a)
    }

    "every member is watched by exactly `monitoredBy` others" in {
      nodes.foreach(n => nodes.count(o => o != n && HeartbeatRing.monitored(o, nodes - o, 4)(n)) shouldBe 4)
    }

    "in a small cluster everybody watches everybody" in {
      val few = nodes.filter(_.address.port <= 3)
      few.foreach(n => HeartbeatRing.monitored(n, few - n, 5) shouldBe few - n)
    }
  }
}
