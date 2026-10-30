package xplat.core.cluster.api

/** Snapshot of the cluster as seen by the local node. */
final case class ClusterState(members: Set[Member], unreachable: Set[Member], leader: Option[Address]) {

  def upMembers: Set[Member] = members.filter(_.isUp)

  def membersWithRole(role: String): Set[Member] = members.filter(_.hasRole(role))

  /** Members that are up and reachable: the ones that can host work. */
  def availableMembers: Set[Member] = upMembers.filterNot(m => unreachable.exists(_.uniqueAddress == m.uniqueAddress))
}

object ClusterState {
  val empty: ClusterState = ClusterState(Set.empty, Set.empty, None)
}
