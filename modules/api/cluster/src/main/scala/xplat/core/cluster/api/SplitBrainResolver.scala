package xplat.core.cluster.api

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

/** Downs members when the cluster splits, so that only one side keeps running.
  *
  * A node cannot tell a crashed member from one cut off by the network. If both sides of a split kept running, both
  * could host the same singleton or sharded entity. When some members stay unreachable for [[stableAfter]], every node
  * runs the [[strategy]] on its own view, and all sides reach compatible decisions without talking to each other.
  *
  * @param stableAfter
  *   time membership and reachability must stay unchanged before deciding. Longer avoids reacting to short glitches;
  *   shorter recovers faster.
  * @param downAllWhenUnstable
  *   if reachability keeps changing for this long after [[stableAfter]], all members are downed, since no safe decision is
  *   possible. `None` keeps waiting.
  */
final case class SplitBrainResolverConfig(
    strategy: SplitBrainStrategy = SplitBrainStrategy.KeepMajority(),
    stableAfter: FiniteDuration = 20.seconds,
    downAllWhenUnstable: Option[FiniteDuration] = Some(15.seconds)
)

/** What to do with the members of a split, from the point of view of the local node. */
sealed trait Decision {

  /** Members to mark as down. The reachable side includes the local node itself. */
  def membersToDown(state: ClusterState): Set[Member] = {
    val unreachableIds = state.unreachable.map(_.uniqueAddress)
    val (unreachable, reachable) = state.members.partition(m => unreachableIds(m.uniqueAddress))
    this match {
      case Decision.DownUnreachable => unreachable
      case Decision.DownReachable   => reachable
      case Decision.DownAll         => state.members
    }
  }
}

object Decision {

  /** This side survives. */
  case object DownUnreachable extends Decision

  /** The other side survives: this node downs itself and the members it can reach. */
  case object DownReachable extends Decision

  /** No side can be trusted: the whole cluster stops. */
  case object DownAll extends Decision
}

/** Picks the surviving side of a split. Either a [[SplitBrainStrategy.LocalStrategy]], decided from the local view
  * alone, or [[SplitBrainStrategy.LeaseMajority]], which asks an external lease.
  */
sealed trait SplitBrainStrategy

object SplitBrainStrategy {

  /** Decides from the local view of the cluster only. It must be a pure function of the state, so the nodes on each side
    * reach compatible decisions without talking to each other. Extend it to write a custom strategy.
    */
  trait LocalStrategy extends SplitBrainStrategy {
    def decide(state: ClusterState): Decision
  }

  /** Members counted in decisions. Joining and WeaklyUp members do not count, but are downed with their side. */
  private val counted: Set[MemberStatus] = Set(MemberStatus.Up, MemberStatus.Leaving, MemberStatus.Exiting)

  private final case class Sides(reachable: Set[Member], unreachable: Set[Member]) {
    def all: Set[Member] = reachable ++ unreachable
  }

  private def sides(state: ClusterState, role: Option[String]): Sides = {
    val unreachableIds = state.unreachable.map(_.uniqueAddress)
    val members = state.members.filter(m => counted(m.status) && role.forall(m.hasRole))
    val (unreachable, reachable) = members.partition(m => unreachableIds(m.uniqueAddress))
    Sides(reachable, unreachable)
  }

  /** The side with more members survives. On an even split, the side with the lowest address survives.
    *
    * Good default for clusters that change size. If the cluster splits in more than two parts and none has a majority,
    * all of them go down.
    */
  final case class KeepMajority(role: Option[String] = None) extends LocalStrategy {
    def decide(state: ClusterState): Decision = {
      val s = sides(state, role)
      val (r, u) = (s.reachable.size, s.unreachable.size)
      if (r > u) Decision.DownUnreachable
      else if (u > r) Decision.DownReachable
      else if (s.all.isEmpty) Decision.DownAll
      else if (s.reachable.contains(s.all.minBy(_.uniqueAddress))) Decision.DownUnreachable
      else Decision.DownReachable
    }
  }

  /** A side survives if it has at least `quorumSize` members.
    *
    * Good for clusters of fixed size. The cluster must never grow past `quorumSize * 2 - 1` members, or both sides could
    * reach quorum; if it does, all members are downed.
    */
  final case class StaticQuorum(quorumSize: Int, role: Option[String] = None) extends LocalStrategy {
    require(quorumSize > 0, s"quorumSize must be positive, got $quorumSize")

    def decide(state: ClusterState): Decision = {
      val s = sides(state, role)
      if (s.all.size > quorumSize * 2 - 1) Decision.DownAll
      else if (s.reachable.size >= quorumSize) Decision.DownUnreachable
      else Decision.DownReachable
    }
  }

  /** The side with the oldest member survives. Useful with singletons, which run on the oldest member, so they do not
    * move.
    *
    * @param downIfAlone
    *   if the oldest member is cut off alone, its side goes down instead and the others survive. Prevents a single node
    *   from taking the whole cluster down.
    */
  final case class KeepOldest(downIfAlone: Boolean = true, role: Option[String] = None) extends LocalStrategy {
    def decide(state: ClusterState): Decision = {
      val s = sides(state, role)
      if (s.all.isEmpty) Decision.DownAll
      else {
        val oldest = s.all.min(Member.ageOrdering)
        val oldestReachable = s.reachable.contains(oldest)
        val (oldestSide, otherSide) = if (oldestReachable) (s.reachable, s.unreachable) else (s.unreachable, s.reachable)
        val oldestSurvives = !(downIfAlone && oldestSide.size == 1 && otherSide.nonEmpty)
        if (oldestSurvives == oldestReachable) Decision.DownUnreachable else Decision.DownReachable
      }
    }
  }

  /** The side that can reach the `referee` node survives.
    *
    * Simple, but the referee is a single point of failure: if it crashes, the whole cluster goes down.
    *
    * @param downAllIfLessThanNodes
    *   if the referee side has fewer members than this, it goes down too.
    */
  final case class KeepReferee(referee: Address, downAllIfLessThanNodes: Int = 1) extends LocalStrategy {
    def decide(state: ClusterState): Decision = {
      val s = sides(state, role = None)
      if (!s.reachable.exists(_.address == referee)) Decision.DownReachable
      else if (s.reachable.size < downAllIfLessThanNodes) Decision.DownAll
      else Decision.DownUnreachable
    }
  }

  /** The side that takes an external lease survives; the other sides down themselves.
    *
    * Always leaves exactly one side, even on even splits or splits in more than two parts, at the cost of depending on the
    * lease store: if it is unavailable during a split, no side gets the lease and the whole cluster goes down.
    *
    * To favour the bigger side, the majority tries at once and the minority waits [[acquireLeaseDelayForMinority]] first.
    * On an even split, the side with the lowest address counts as the majority. The implementation runs:
    * {{{
    * sleep(strategy.acquireDelay(state)) >> lease.acquire.map(strategy.decide(state, _))
    * }}}
    * It needs a `LeaseProvider[F]`, given to the `ClusterBuilder` implementation.
    *
    * @param leaseName
    *   name of the lease in the store; the same on every node of the cluster.
    */
  final case class LeaseMajority(
      leaseName: String,
      acquireLeaseDelayForMinority: FiniteDuration = 2.seconds,
      role: Option[String] = None
  ) extends SplitBrainStrategy {

    /** Whether the local side is the majority (or wins the tie on an even split). */
    def isMajority(state: ClusterState): Boolean = KeepMajority(role).decide(state) == Decision.DownUnreachable

    /** How long the local side waits before trying to take the lease. */
    def acquireDelay(state: ClusterState): FiniteDuration = if (isMajority(state)) Duration.Zero else acquireLeaseDelayForMinority

    def decide(state: ClusterState, leaseAcquired: Boolean): Decision =
      if (leaseAcquired) Decision.DownUnreachable else Decision.DownReachable
  }

  /** Downs every member on any split. Safe when restarting the whole cluster is cheaper than any risk of two sides. */
  case object DownAllNodes extends LocalStrategy {
    def decide(state: ClusterState): Decision = Decision.DownAll
  }
}
