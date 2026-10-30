package xplat.core.io.cluster.internal

import xplat.core.cluster.api.MemberStatus._
import xplat.core.cluster.api._

/** Version of the gossip: one counter per node that changed it. */
final case class VectorClock(versions: Map[UniqueAddress, Long]) {

  def increment(node: UniqueAddress): VectorClock = VectorClock(versions.updated(node, versions.getOrElse(node, 0L) + 1))

  def merge(other: VectorClock): VectorClock =
    VectorClock((versions.keySet ++ other.versions.keySet).map(k => k -> math.max(versions.getOrElse(k, 0L), other.versions.getOrElse(k, 0L))).toMap)

  def compare(other: VectorClock): VectorClock.Order = {
    val keys = versions.keySet ++ other.versions.keySet
    val less = keys.exists(k => versions.getOrElse(k, 0L) < other.versions.getOrElse(k, 0L))
    val greater = keys.exists(k => versions.getOrElse(k, 0L) > other.versions.getOrElse(k, 0L))
    (less, greater) match {
      case (false, false) => VectorClock.Same
      case (true, false)  => VectorClock.Before
      case (false, true)  => VectorClock.After
      case (true, true)   => VectorClock.Concurrent
    }
  }
}

object VectorClock {
  val empty: VectorClock = VectorClock(Map.empty)

  sealed trait Order
  case object Same extends Order
  case object Before extends Order
  case object After extends Order
  case object Concurrent extends Order
}

/** Members an observer cannot reach. Only the observer changes its entry, bumping `version` each time. */
final case class Reach(version: Long, unreachable: Set[UniqueAddress])

/** Cluster view shared between nodes by gossip.
  *
  * @param tombstones
  *   members that were removed. They are never added back, so a stale gossip cannot bring them back to life.
  * @param seen
  *   nodes that have seen this exact version. When every reachable member has, the gossip has converged and the leader
  *   can change member statuses.
  */
final case class GossipState(
    members: Map[UniqueAddress, Member],
    reachability: Map[UniqueAddress, Reach],
    tombstones: Set[UniqueAddress],
    version: VectorClock,
    seen: Set[UniqueAddress]
) {

  /** Members some live observer cannot reach. */
  lazy val unreachable: Set[UniqueAddress] =
    reachability.iterator
      .collect { case (observer, reach) if members.get(observer).exists(_.status != Down) => reach.unreachable }
      .flatten
      .filter(members.contains)
      .toSet

  def reachableMembers: Iterable[Member] = members.values.filterNot(m => unreachable(m.uniqueAddress))

  /** The member that changes statuses: the reachable Up or Leaving member with the lowest address. If there is none, a
    * joining or exiting one, so a single node can still join and leave.
    */
  lazy val leader: Option[UniqueAddress] = {
    val candidates = reachableMembers.filter(_.status != Down)
    val preferred = candidates.filter(m => m.status == Up || m.status == Leaving)
    (if (preferred.nonEmpty) preferred else candidates).map(_.uniqueAddress).minOption
  }

  /** Every reachable member has seen this version, and unreachable members are only down or exiting ones. */
  lazy val convergence: Boolean =
    unreachable.forall(id => members.get(id).forall(m => m.status == Down || m.status == Exiting)) &&
      members.values.filter(m => m.status != Down && !unreachable(m.uniqueAddress)).forall(m => seen(m.uniqueAddress))

  def view: ClusterState =
    ClusterState(members.values.toSet, members.values.filter(m => unreachable(m.uniqueAddress)).toSet, leader.flatMap(members.get).map(_.address))

  /** Applies a local change made by `self`: the version moves forward and only `self` has seen it. */
  def changed(self: UniqueAddress)(f: GossipState => GossipState): GossipState = {
    val next = f(this)
    if (next.members == members && next.reachability == reachability && next.tombstones == tombstones) this
    else next.copy(version = version.increment(self), seen = Set(self))
  }

  def withMember(member: Member): GossipState =
    if (tombstones(member.uniqueAddress)) this else copy(members = members.updated(member.uniqueAddress, member))

  def withStatus(id: UniqueAddress, status: MemberStatus): GossipState =
    members.get(id).fold(this)(m => copy(members = members.updated(id, m.copy(status = status))))

  def removed(ids: Iterable[UniqueAddress]): GossipState =
    copy(members = members -- ids, reachability = reachability -- ids, tombstones = tombstones ++ ids)

  /** Records what `observer` can not reach. */
  def withObservation(observer: UniqueAddress, unreachableNow: Set[UniqueAddress]): GossipState = {
    val current = reachability.get(observer)
    if (current.map(_.unreachable).getOrElse(Set.empty) == unreachableNow) this
    else copy(reachability = reachability.updated(observer, Reach(current.fold(1L)(_.version + 1), unreachableNow)))
  }

  /** Combines two views that changed concurrently. */
  def merge(other: GossipState): GossipState = {
    val tombs = tombstones ++ other.tombstones
    val ids = (members.keySet ++ other.members.keySet) -- tombs
    val mergedMembers = ids.iterator.map { id =>
      val m = (members.get(id), other.members.get(id)) match {
        case (Some(a), Some(b)) => GossipState.latest(a, b)
        case (a, b)             => a.orElse(b).get
      }
      id -> m
    }.toMap
    val observers = (reachability.keySet ++ other.reachability.keySet) -- tombs
    val mergedReach = observers.iterator.map { o =>
      o -> Seq(reachability.get(o), other.reachability.get(o)).flatten.maxBy(_.version)
    }.toMap
    GossipState(mergedMembers, mergedReach, tombs, version.merge(other.version), Set.empty)
  }

  /** Gossip after `self` receives `remote`. */
  def receive(self: UniqueAddress, remote: GossipState): GossipState =
    version.compare(remote.version) match {
      case VectorClock.Same       => copy(seen = seen ++ remote.seen + self)
      case VectorClock.Before     => remote.copy(seen = remote.seen + self)
      case VectorClock.After      => this
      case VectorClock.Concurrent => merge(remote).copy(seen = Set(self))
    }

  /** Status changes made by the leader, if `self` is the leader. With convergence: joining members become Up, leaving
    * ones Exiting, and exiting or down ones are removed. Without it, only joining members that are reachable become
    * WeaklyUp, so the cluster can grow while a member is unreachable.
    */
  def leaderActions(self: UniqueAddress): GossipState =
    if (!leader.contains(self)) this
    else if (convergence) {
      changed(self) { g =>
        val toRemove = g.members.values.filter(m => m.status == Exiting || m.status == Down).map(_.uniqueAddress)
        var upNumber = g.members.values.map(_.upNumber).filter(_ != Int.MaxValue).maxOption.getOrElse(0)
        val updated = g.members.values.toList.sortBy(_.uniqueAddress).map { m =>
          m.status match {
            case Joining | WeaklyUp =>
              val n = if (m.upNumber != Int.MaxValue) m.upNumber else { upNumber += 1; upNumber }
              m.copy(status = Up, upNumber = n)
            case Leaving => m.copy(status = Exiting)
            case _       => m
          }
        }
        g.copy(members = updated.map(m => m.uniqueAddress -> m).toMap).removed(toRemove)
      }
    } else if (unreachable.nonEmpty) {
      changed(self) { g =>
        val weaklyUp = g.members.values.filter(m => m.status == Joining && !unreachable(m.uniqueAddress))
        weaklyUp.foldLeft(g)((acc, m) => acc.withStatus(m.uniqueAddress, WeaklyUp))
      }
    } else this
}

object GossipState {
  val empty: GossipState = GossipState(Map.empty, Map.empty, Set.empty, VectorClock.empty, Set.empty)

  private val precedence: Map[MemberStatus, Int] =
    Map(Joining -> 0, WeaklyUp -> 1, Up -> 2, Leaving -> 3, Exiting -> 4, Down -> 5, Removed -> 6)

  /** Statuses only move forward, so on conflict the later one wins. */
  def latest(a: Member, b: Member): Member = {
    val (pa, pb) = (precedence(a.status), precedence(b.status))
    if (pa != pb) { if (pa > pb) a else b }
    else if (a.upNumber <= b.upNumber) a
    else b
  }
}
