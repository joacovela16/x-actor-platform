package xplat.core.io.cluster.internal

import xplat.core.cluster.api._
import xplat.core.io.net.{BinReader, BinWriter}

/** Messages exchanged between cluster nodes. */
sealed trait Wire

object Wire {
  final case class Join(node: UniqueAddress, roles: Set[String]) extends Wire
  final case class Gossip(from: UniqueAddress, gossip: GossipState) extends Wire
  final case class Heartbeat(from: UniqueAddress) extends Wire

  /** A message for the endpoint registered at `path` on the receiving node. */
  final case class Deliver(path: String, payload: Array[Byte]) extends Wire

  /** A request that expects a [[Reply]] (or a [[Failure]]) with the same id, sent back to `from`. */
  final case class Request(from: Address, id: Long, path: String, payload: Array[Byte]) extends Wire
  final case class Reply(id: Long, payload: Array[Byte]) extends Wire
  final case class Failure(id: Long, message: String) extends Wire

  /** Receptionist entries registered on `from`: service key id -> endpoint paths. */
  final case class Registry(from: UniqueAddress, version: Long, entries: Map[String, Set[String]]) extends Wire

  private val statuses: Vector[MemberStatus] = {
    import MemberStatus._
    Vector(Joining, WeaklyUp, Up, Leaving, Exiting, Down, Removed)
  }

  private def address(w: BinWriter, a: Address): Unit = { w.string(a.system).string(a.host).int(a.port); () }
  private def address(r: BinReader): Address = Address(r.string(), r.string(), r.int())

  private def unique(w: BinWriter, u: UniqueAddress): Unit = { address(w, u.address); w.long(u.uid); () }
  private def unique(r: BinReader): UniqueAddress = UniqueAddress(address(r), r.long())

  private def member(w: BinWriter, m: Member): Unit = {
    unique(w, m.uniqueAddress)
    w.byte(statuses.indexOf(m.status)).seq(m.roles)(_.string(_)).int(m.upNumber)
    ()
  }
  private def member(r: BinReader): Member = Member(unique(r), statuses(r.byte()), r.seq(_.string()).toSet, r.int())

  private def gossip(w: BinWriter, g: GossipState): Unit = {
    w.seq(g.members.values)(member)
    w.seq(g.reachability) { case (w, (observer, reach)) =>
      unique(w, observer); w.long(reach.version).seq(reach.unreachable)(unique)
    }
    w.seq(g.tombstones)(unique)
    w.seq(g.version.versions) { case (w, (node, v)) => unique(w, node); w.long(v) }
    w.seq(g.seen)(unique)
    ()
  }

  private def gossip(r: BinReader): GossipState = {
    val members = r.seq(member).map(m => m.uniqueAddress -> m).toMap
    val reach = r.seq(r => unique(r) -> Reach(r.long(), r.seq(unique).toSet)).toMap
    val tombs = r.seq(unique).toSet
    val version = VectorClock(r.seq(r => unique(r) -> r.long()).toMap)
    val seen = r.seq(unique).toSet
    GossipState(members, reach, tombs, version, seen)
  }

  def encode(wire: Wire): Array[Byte] = {
    val w = BinWriter()
    wire match {
      case Join(node, roles)    => w.byte(1); unique(w, node); w.seq(roles)(_.string(_))
      case Gossip(from, g)      => w.byte(2); unique(w, from); gossip(w, g)
      case Heartbeat(from)      => w.byte(3); unique(w, from)
      case Deliver(path, bytes) => w.byte(4).string(path).bytes(bytes)
      case Request(from, id, path, bytes) =>
        w.byte(5); address(w, from); w.long(id).string(path).bytes(bytes)
      case Reply(id, bytes)       => w.byte(6).long(id).bytes(bytes)
      case Failure(id, message)   => w.byte(7).long(id).string(message)
      case Registry(from, v, map) =>
        w.byte(8); unique(w, from); w.long(v).seq(map) { case (w, (k, paths)) => w.string(k).seq(paths)(_.string(_)) }
    }
    w.result()
  }

  def decode(bytes: Array[Byte]): Wire = {
    val r = BinReader(bytes)
    r.byte() match {
      case 1     => Join(unique(r), r.seq(_.string()).toSet)
      case 2     => Gossip(unique(r), gossip(r))
      case 3     => Heartbeat(unique(r))
      case 4     => Deliver(r.string(), r.bytes())
      case 5     => Request(address(r), r.long(), r.string(), r.bytes())
      case 6     => Reply(r.long(), r.bytes())
      case 7     => Failure(r.long(), r.string())
      case 8     => Registry(unique(r), r.long(), r.seq(r => r.string() -> r.seq(_.string()).toSet).toMap)
      case other => throw new IllegalArgumentException(s"unknown message type $other")
    }
  }
}
