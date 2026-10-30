package xplat.core.coordination

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class LeaseSettingsTest extends AnyFunSuite with Matchers {

  test("defaults renew well before the lease expires") {
    val settings = LeaseSettings("lease", "node-1")
    settings.heartbeatInterval should be < settings.timeToLive
  }

  test("rejects a heartbeat that is not shorter than the time to live") {
    an[IllegalArgumentException] should be thrownBy LeaseSettings("lease", "node-1", timeToLive = 10.seconds, heartbeatInterval = 10.seconds)
  }
}
