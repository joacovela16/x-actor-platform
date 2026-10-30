package xplat.core.cluster.api

/** @param upNumber
  *   order in which members became Up: lower is older. Members that are not Up yet have `Int.MaxValue`.
  */
final case class Member(uniqueAddress: UniqueAddress, status: MemberStatus, roles: Set[String], upNumber: Int = Int.MaxValue) {
  def address: Address = uniqueAddress.address
  def hasRole(role: String): Boolean = roles.contains(role)
  def isUp: Boolean = status == MemberStatus.Up || status == MemberStatus.WeaklyUp
}

object Member {

  /** Oldest member first. The address breaks ties, so every node picks the same oldest member. */
  val ageOrdering: Ordering[Member] = Ordering.by(m => (m.upNumber, m.uniqueAddress))
}
