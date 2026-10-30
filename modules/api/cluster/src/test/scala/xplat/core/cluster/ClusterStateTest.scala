package xplat.core.cluster

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import xplat.core.cluster.api._

class ClusterStateTest extends AnyFunSuite with Matchers {

  private def member(port: Int, status: MemberStatus, roles: String*): Member =
    Member(UniqueAddress(Address("sys", "127.0.0.1", port), uid = port.toLong), status, roles.toSet)

  private val joining = member(1, MemberStatus.Joining)
  private val weaklyUp = member(2, MemberStatus.WeaklyUp, "backend")
  private val up = member(3, MemberStatus.Up, "backend")
  private val upFrontend = member(4, MemberStatus.Up, "frontend")
  private val leaving = member(5, MemberStatus.Leaving, "backend")

  private val state = ClusterState(Set(joining, weaklyUp, up, upFrontend, leaving), unreachable = Set(upFrontend), leader = Some(up.address))

  test("upMembers includes Up and WeaklyUp members only") {
    state.upMembers shouldBe Set(weaklyUp, up, upFrontend)
  }

  test("membersWithRole filters by role whatever the status") {
    state.membersWithRole("backend") shouldBe Set(weaklyUp, up, leaving)
  }

  test("availableMembers excludes unreachable members") {
    state.availableMembers shouldBe Set(weaklyUp, up)
  }

  test("a restarted node on the same address is a different member") {
    val restarted = up.copy(uniqueAddress = up.uniqueAddress.copy(uid = 99L))
    ClusterState(Set(restarted), unreachable = Set(up), leader = None).availableMembers shouldBe Set(restarted)
  }

  test("address renders as a URI") {
    Address("sys", "10.0.0.1", 2552).toString shouldBe "xplat://sys@10.0.0.1:2552"
  }
}
