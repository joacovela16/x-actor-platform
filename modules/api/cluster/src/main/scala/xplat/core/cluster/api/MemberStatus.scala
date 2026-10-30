package xplat.core.cluster.api

/** Lifecycle of a cluster member: Joining -> (WeaklyUp) -> Up -> Leaving -> Exiting -> Removed, or Down -> Removed. */
sealed trait MemberStatus

object MemberStatus {
  case object Joining extends MemberStatus

  /** Joined while the cluster could not converge (some member unreachable). Usable, but not counted for decisions. */
  case object WeaklyUp extends MemberStatus
  case object Up extends MemberStatus
  case object Leaving extends MemberStatus
  case object Exiting extends MemberStatus

  /** Marked as down (crashed or unreachable for too long). It will be removed and cannot come back. */
  case object Down extends MemberStatus
  case object Removed extends MemberStatus
}
