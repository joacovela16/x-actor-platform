package xplat.core.io.cluster.internal

import xplat.core.cluster.api.ClusterEvent._
import xplat.core.cluster.api._

/** Turns changes of the cluster view into events. */
object Events {

  def statusEvent(m: Member): Option[ClusterEvent] = m.status match {
    case MemberStatus.Joining  => Some(MemberJoined(m))
    case MemberStatus.WeaklyUp => Some(MemberWeaklyUp(m))
    case MemberStatus.Up       => Some(MemberUp(m))
    case MemberStatus.Leaving  => Some(MemberLeft(m))
    case MemberStatus.Exiting  => Some(MemberExited(m))
    case MemberStatus.Down     => Some(MemberDowned(m))
    case MemberStatus.Removed  => None
  }

  def diff(before: ClusterState, after: ClusterState): Vector[ClusterEvent] = {
    val oldById = before.members.map(m => m.uniqueAddress -> m).toMap
    val newById = after.members.map(m => m.uniqueAddress -> m).toMap

    val changed = after.members.toVector.sortBy(_.uniqueAddress).flatMap { m =>
      if (oldById.get(m.uniqueAddress).exists(_.status == m.status)) None else statusEvent(m)
    }
    val removed = before.members.toVector.sortBy(_.uniqueAddress).collect {
      case m if !newById.contains(m.uniqueAddress) => MemberRemoved(m.copy(status = MemberStatus.Removed), m.status)
    }

    val oldUnreachable = before.unreachable.map(_.uniqueAddress)
    val newUnreachable = after.unreachable.map(_.uniqueAddress)
    val unreachable = after.unreachable.toVector.sortBy(_.uniqueAddress).collect {
      case m if !oldUnreachable(m.uniqueAddress) => UnreachableMember(m)
    }
    val reachable = before.unreachable.toVector.sortBy(_.uniqueAddress).collect {
      case m if !newUnreachable(m.uniqueAddress) && newById.contains(m.uniqueAddress) => ReachableMember(newById(m.uniqueAddress))
    }

    val leader = if (before.leader != after.leader) Vector(LeaderChanged(after.leader)) else Vector.empty

    changed ++ removed ++ unreachable ++ reachable ++ leader
  }

  /** The current state as events, for a new subscriber. */
  def initial(state: ClusterState): Vector[ClusterEvent] = diff(ClusterState.empty, state)
}
