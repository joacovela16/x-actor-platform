package xplat.core.cluster

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.cluster.api.Decision._
import xplat.core.cluster.api.SplitBrainStrategy._
import xplat.core.cluster.api._

import scala.concurrent.duration.{Duration, DurationInt}

class SplitBrainStrategyTest extends AnyFreeSpec with Matchers {

  /** Member on port `n`, which became Up in that order: lower `n` is older and has a lower address. */
  private def node(n: Int, roles: String*): Member =
    Member(UniqueAddress(Address("sys", "10.0.0.1", n), uid = n.toLong), MemberStatus.Up, roles.toSet, upNumber = n)

  private def joining(n: Int): Member = node(n).copy(status = MemberStatus.Joining, upNumber = Int.MaxValue)

  /** Cluster as seen from `side`: every other member is unreachable. */
  private def viewFrom(side: Set[Member], others: Set[Member]): ClusterState =
    ClusterState(side ++ others, unreachable = others, leader = None)

  /** Decisions of both sides of a split, each taken on its own view. */
  private def split(strategy: LocalStrategy, a: Set[Member], b: Set[Member]): (Decision, Decision) =
    (strategy.decide(viewFrom(a, b)), strategy.decide(viewFrom(b, a)))

  private val nodes: Map[Int, Member] = (1 to 6).map(n => n -> node(n)).toMap
  private def side(ns: Int*): Set[Member] = ns.map(nodes).toSet

  "on a two-way split, both sides never survive" in {
    val strategies = Seq[LocalStrategy](KeepMajority(), StaticQuorum(3), KeepOldest(), KeepOldest(downIfAlone = false), KeepReferee(nodes(1).address), DownAllNodes)
    val splits = Seq(
      side(1, 2, 3) -> side(4, 5),
      side(1, 2) -> side(3, 4),
      side(1) -> side(2, 3, 4, 5),
      side(5) -> side(1, 2, 3, 4),
      side(2, 4) -> side(1, 3, 5)
    )
    for (strategy <- strategies; (a, b) <- splits) {
      val decisions = split(strategy, a, b)
      withClue(s"$strategy on ${a.map(_.address.port)} / ${b.map(_.address.port)}: ") {
        decisions should not be (DownUnreachable, DownUnreachable)
      }
    }
  }

  "KeepMajority" - {

    "the bigger side survives" in {
      split(KeepMajority(), side(1, 2, 3), side(4, 5)) shouldBe (DownUnreachable, DownReachable)
    }

    "on an even split, the side with the lowest address survives" in {
      split(KeepMajority(), side(2, 3), side(1, 4)) shouldBe (DownReachable, DownUnreachable)
    }

    "only members with the role are counted" in {
      val backends = Set(node(1, "backend"), node(2, "backend"), node(3, "backend"))
      val a = Set(node(4), node(5), node(6), backends.find(_.address.port == 1).get)
      val b = backends.filterNot(_.address.port == 1)
      split(KeepMajority(role = Some("backend")), a, b) shouldBe (DownReachable, DownUnreachable)
    }

    "joining members are not counted but are downed with their side" in {
      val a = side(1, 2) + joining(10) + joining(11)
      val b = side(3, 4, 5)
      val view = viewFrom(a, b)
      KeepMajority().decide(view) shouldBe DownReachable
      DownReachable.membersToDown(view) shouldBe a
    }
  }

  "StaticQuorum" - {

    "the side with quorum survives" in {
      split(StaticQuorum(3), side(1, 2), side(3, 4, 5)) shouldBe (DownReachable, DownUnreachable)
    }

    "downs everything when the cluster is too big for the quorum" in {
      StaticQuorum(3).decide(viewFrom(side(1, 2, 3), side(4, 5, 6))) shouldBe DownAll
    }

    "rejects a non positive quorum" in {
      an[IllegalArgumentException] should be thrownBy StaticQuorum(0)
    }
  }

  "KeepOldest" - {

    "the side with the oldest member survives, even if it is smaller" in {
      split(KeepOldest(), side(1, 5), side(2, 3, 4)) shouldBe (DownUnreachable, DownReachable)
    }

    "if the oldest is cut off alone, the others survive" in {
      split(KeepOldest(), side(1), side(2, 3, 4)) shouldBe (DownReachable, DownUnreachable)
    }

    "without downIfAlone, the oldest survives even alone" in {
      split(KeepOldest(downIfAlone = false), side(1), side(2, 3, 4)) shouldBe (DownUnreachable, DownReachable)
    }
  }

  "KeepReferee" - {

    "the side that reaches the referee survives" in {
      split(KeepReferee(nodes(3).address), side(1, 2), side(3, 4)) shouldBe (DownReachable, DownUnreachable)
    }

    "the referee side goes down too if it is too small" in {
      KeepReferee(nodes(1).address, downAllIfLessThanNodes = 3).decide(viewFrom(side(1, 2), side(3, 4))) shouldBe DownAll
    }
  }

  "LeaseMajority" - {
    val strategy = LeaseMajority("sbr", acquireLeaseDelayForMinority = 3.seconds)

    "the majority tries at once and the minority waits" in {
      strategy.acquireDelay(viewFrom(side(1, 2, 3), side(4, 5))) shouldBe Duration.Zero
      strategy.acquireDelay(viewFrom(side(4, 5), side(1, 2, 3))) shouldBe 3.seconds
    }

    "on an even split, the side with the lowest address counts as the majority" in {
      strategy.isMajority(viewFrom(side(1, 4), side(2, 3))) shouldBe true
      strategy.isMajority(viewFrom(side(2, 3), side(1, 4))) shouldBe false
    }

    "the side that gets the lease survives and the other downs itself" in {
      val view = viewFrom(side(4, 5), side(1, 2, 3))
      strategy.decide(view, leaseAcquired = true) shouldBe DownUnreachable
      strategy.decide(view, leaseAcquired = false) shouldBe DownReachable
    }

    "with a lease only one owner can hold, exactly one side survives" in {
      for ((a, b) <- Seq(side(1, 2, 3) -> side(4, 5), side(1, 2) -> side(3, 4), side(5) -> side(1, 2, 3, 4))) {
        // The side that asks first gets the lease: the one with the shorter delay.
        val (viewA, viewB) = (viewFrom(a, b), viewFrom(b, a))
        val aFirst = strategy.acquireDelay(viewA) < strategy.acquireDelay(viewB)
        val decisions = (strategy.decide(viewA, leaseAcquired = aFirst), strategy.decide(viewB, leaseAcquired = !aFirst))
        decisions should (equal((DownUnreachable, DownReachable)) or equal((DownReachable, DownUnreachable)))
        // The bigger side (or the lowest address on a tie) is the one that survives.
        aFirst shouldBe strategy.isMajority(viewA)
      }
    }
  }

  "DownAllNodes downs every member" in {
    val view = viewFrom(side(1, 2, 3), side(4))
    DownAllNodes.decide(view) shouldBe DownAll
    DownAll.membersToDown(view) shouldBe side(1, 2, 3, 4)
  }

  "DownUnreachable downs only the unreachable members" in {
    DownUnreachable.membersToDown(viewFrom(side(1, 2, 3), side(4, 5))) shouldBe side(4, 5)
  }
}
