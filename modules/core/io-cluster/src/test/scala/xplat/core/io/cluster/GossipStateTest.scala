package xplat.core.io.cluster

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.cluster.api.MemberStatus._
import xplat.core.cluster.api._
import xplat.core.io.cluster.internal._

class GossipStateTest extends AnyFreeSpec with Matchers {

  private def id(n: Int): UniqueAddress = UniqueAddress(Address("sys", "127.0.0.1", n), n.toLong)
  private def member(n: Int, status: MemberStatus, upNumber: Int = Int.MaxValue): Member = Member(id(n), status, Set.empty, upNumber)

  private def gossip(members: Member*): GossipState =
    GossipState.empty.copy(members = members.map(m => m.uniqueAddress -> m).toMap)

  /** Gossip that every member has seen. */
  private def converged(members: Member*): GossipState = {
    val g = gossip(members: _*)
    g.copy(seen = g.members.keySet)
  }

  "VectorClock" - {
    val a = VectorClock.empty.increment(id(1))
    val b = VectorClock.empty.increment(id(2))

    "orders versions" in {
      a.compare(a) shouldBe VectorClock.Same
      a.compare(a.increment(id(1))) shouldBe VectorClock.Before
      a.increment(id(1)).compare(a) shouldBe VectorClock.After
      a.compare(b) shouldBe VectorClock.Concurrent
    }

    "a merged version is after both" in {
      val merged = a.merge(b)
      (merged.compare(a), merged.compare(b)) shouldBe (VectorClock.After, VectorClock.After)
    }
  }

  "receive" - {
    val base = gossip(member(1, Up, 1), member(2, Up, 2)).changed(id(1))(identity).copy(version = VectorClock.empty.increment(id(1)), seen = Set(id(1)))

    "adopts a newer gossip and marks it as seen" in {
      val newer = base.changed(id(2))(_.withStatus(id(2), Leaving))
      base.receive(id(1), newer) shouldBe newer.copy(seen = Set(id(1), id(2)))
    }

    "keeps its own gossip when it is newer" in {
      val newer = base.changed(id(1))(_.withStatus(id(2), Leaving))
      newer.receive(id(1), base) shouldBe newer
    }

    "merges concurrent changes, keeping the later status" in {
      val left = base.changed(id(1))(_.withStatus(id(2), Leaving))
      val right = base.changed(id(2))(_.withMember(member(3, Joining)))
      val merged = left.receive(id(1), right)
      merged.members.values.map(m => m.uniqueAddress -> m.status).toMap shouldBe Map(id(1) -> Up, id(2) -> Leaving, id(3) -> Joining)
      merged.version.compare(left.version) shouldBe VectorClock.After
      merged.seen shouldBe Set(id(1))
    }

    "never brings back a removed member" in {
      val removed = base.changed(id(1))(_.removed(Seq(id(2))))
      val stale = base.changed(id(2))(_.withStatus(id(2), Leaving))
      removed.receive(id(1), stale).members.keySet shouldBe Set(id(1))
    }

    "merging the same versions marks them as seen by both" in {
      base.receive(id(2), base.copy(seen = Set(id(1)))).seen shouldBe Set(id(1), id(2))
    }
  }

  "reachability" - {
    "a member is unreachable if a live observer says so" in {
      val g = gossip(member(1, Up), member(2, Up), member(3, Up)).withObservation(id(1), Set(id(3)))
      g.unreachable shouldBe Set(id(3))
      g.view.unreachable.map(_.uniqueAddress) shouldBe Set(id(3))
    }

    "observations of down members are ignored" in {
      val g = gossip(member(1, Down), member(2, Up)).withObservation(id(1), Set(id(2)))
      g.unreachable shouldBe empty
    }

    "an observation that does not change keeps its version" in {
      val g = gossip(member(1, Up)).withObservation(id(1), Set(id(2)))
      g.withObservation(id(1), Set(id(2))) shouldBe g
    }
  }

  "leader and convergence" - {
    "the leader is the reachable Up member with the lowest address" in {
      gossip(member(3, Up), member(2, Up), member(1, Joining)).leader shouldBe Some(id(2))
      gossip(member(1, Up), member(2, Up)).withObservation(id(2), Set(id(1))).leader shouldBe Some(id(2))
    }

    "a lone joining node leads itself" in {
      gossip(member(1, Joining)).leader shouldBe Some(id(1))
    }

    "convergence needs every reachable member to have seen the gossip" in {
      gossip(member(1, Up), member(2, Up)).copy(seen = Set(id(1))).convergence shouldBe false
      converged(member(1, Up), member(2, Up)).convergence shouldBe true
    }

    "an unreachable member blocks convergence unless it is down" in {
      converged(member(1, Up), member(2, Up)).withObservation(id(1), Set(id(2))).convergence shouldBe false
      converged(member(1, Up), member(2, Down)).withObservation(id(1), Set(id(2))).convergence shouldBe true
    }
  }

  "leader actions" - {
    "move joining members to Up with increasing up numbers" in {
      val g = converged(member(1, Up, 1), member(3, Joining), member(2, Joining)).leaderActions(id(1))
      g.members.values.map(m => m.uniqueAddress -> (m.status, m.upNumber)).toMap shouldBe
        Map(id(1) -> (Up, 1), id(2) -> (Up, 2), id(3) -> (Up, 3))
    }

    "move leaving members to Exiting, then remove them" in {
      val exiting = converged(member(1, Up, 1), member(2, Leaving, 2)).leaderActions(id(1))
      exiting.members(id(2)).status shouldBe Exiting
      val removed = exiting.copy(seen = exiting.members.keySet).leaderActions(id(1))
      (removed.members.keySet, removed.tombstones) shouldBe (Set(id(1)), Set(id(2)))
    }

    "remove down members even if unreachable" in {
      val g = converged(member(1, Up, 1), member(2, Down, 2)).withObservation(id(1), Set(id(2)))
      g.leaderActions(id(1)).members.keySet shouldBe Set(id(1))
    }

    "only the leader acts" in {
      val g = converged(member(1, Up, 1), member(2, Joining))
      g.leaderActions(id(2)) shouldBe g
    }

    "without convergence, reachable joining members become WeaklyUp" in {
      val g = converged(member(1, Up, 1), member(2, Up, 2), member(3, Joining)).withObservation(id(1), Set(id(2)))
      g.leaderActions(id(1)).members(id(3)).status shouldBe WeaklyUp
    }

    "a single node joins and leaves alone" in {
      val up = converged(member(1, Joining)).leaderActions(id(1))
      up.members(id(1)).status shouldBe Up
      val leaving = up.changed(id(1))(_.withStatus(id(1), Leaving))
      val exiting = leaving.leaderActions(id(1))
      exiting.leaderActions(id(1)).tombstones shouldBe Set(id(1))
    }
  }

  "wire messages survive encoding" in {
    val g = converged(member(1, Up, 1), member(2, Leaving, 2)).withObservation(id(1), Set(id(2))).changed(id(1))(_.removed(Seq(id(9))))
    val messages = Seq[Wire](
      Wire.Join(id(1), Set("a", "b")),
      Wire.Gossip(id(1), g),
      Wire.Heartbeat(id(2)),
      Wire.Request(id(1).address, 7L, "/p", Array[Byte](1, 2)),
      Wire.Reply(7L, Array[Byte](3)),
      Wire.Failure(7L, "boom"),
      Wire.Registry(id(1), 3L, Map("k" -> Set("/a", "/b")))
    )
    // Messages with byte arrays are compared through their encoding, since arrays have no structural equality.
    messages.foreach(m => Wire.encode(Wire.decode(Wire.encode(m))).toSeq shouldBe Wire.encode(m).toSeq)
    Wire.decode(Wire.encode(Wire.Gossip(id(1), g))) shouldBe Wire.Gossip(id(1), g)
    val Wire.Deliver(path, bytes) = Wire.decode(Wire.encode(Wire.Deliver("/x", Array[Byte](5, 6))))
    (path, bytes.toSeq) shouldBe ("/x", Seq[Byte](5, 6))
  }
}
