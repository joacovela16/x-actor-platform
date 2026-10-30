package xplat.core.cluster.api

/** Changes in the cluster, published through [[xplat.core.cluster.Cluster.events]]. */
sealed trait ClusterEvent

object ClusterEvent {

  /** Membership changes. */
  sealed trait MemberEvent extends ClusterEvent {
    def member: Member
  }

  final case class MemberJoined(member: Member) extends MemberEvent
  final case class MemberWeaklyUp(member: Member) extends MemberEvent
  final case class MemberUp(member: Member) extends MemberEvent
  final case class MemberLeft(member: Member) extends MemberEvent
  final case class MemberExited(member: Member) extends MemberEvent
  final case class MemberDowned(member: Member) extends MemberEvent
  final case class MemberRemoved(member: Member, previousStatus: MemberStatus) extends MemberEvent

  /** Failure detector changes. A member can become unreachable and reachable again without changing its status. */
  sealed trait ReachabilityEvent extends ClusterEvent {
    def member: Member
  }

  final case class UnreachableMember(member: Member) extends ReachabilityEvent
  final case class ReachableMember(member: Member) extends ReachabilityEvent

  /** The leader decides membership transitions (e.g. Joining -> Up). It is `None` while there is no convergence. */
  final case class LeaderChanged(leader: Option[Address]) extends ClusterEvent
}
