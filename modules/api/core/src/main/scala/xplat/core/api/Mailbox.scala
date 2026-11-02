package xplat.core.api

/** How many messages may wait in an actor's mailbox, and what happens to a new one once that limit is reached.
  *
  * Only messages sent through an [[ActorRef]] count towards the limit. Signals, stops and the messages an actor sends to
  * itself through its [[Context]] (pipe, timers, unstash) are always accepted, so an actor never drops or rejects its own
  * work. All of them keep the order in which they arrived.
  */
sealed trait Mailbox

object Mailbox {

  /** No limit: messages wait in memory until the actor handles them. */
  case object Unbounded extends Mailbox

  final case class Bounded(capacity: Int, overflow: Overflow) extends Mailbox {
    require(capacity > 0, s"Mailbox capacity must be positive, got $capacity")
  }

  def bounded(capacity: Int, overflow: Overflow): Mailbox = Bounded(capacity, overflow)

  sealed trait Overflow

  object Overflow {

    /** Keeps the waiting messages and discards the new one. */
    case object DropNew extends Overflow

    /** Discards the oldest waiting message to make room for the new one. */
    case object DropOldest extends Overflow

    /** Rejects the new message: `send` fails with [[xplat.core.api.error.MailboxOverflow]]. */
    case object Fail extends Overflow
  }

}
