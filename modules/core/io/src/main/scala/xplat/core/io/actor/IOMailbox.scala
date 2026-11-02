package xplat.core.io.actor

import cats.effect.kernel.{Async, Deferred, Ref}
import cats.implicits._
import fs2.Stream
import xplat.core.api.error.MailboxOverflow
import xplat.core.api.{Logger, Mailbox}

import scala.collection.immutable.Queue

/** Mailbox with two queues. `system` takes signals, stops and the messages an actor sends to itself, and is never
  * limited, so an actor can always talk to itself. `user` takes messages sent through a ref and applies the configured
  * [[Mailbox]] limit. Every element gets a number from one shared sequence, and `take` returns the lowest one, so the
  * reader sees elements in the order they arrived.
  *
  * Once closed, offers are refused, and `take` returns what is left before ending.
  */
private[actor] final case class IOMailbox[F[_], A](private val state: Ref[F, IOMailbox.State[F, A]], config: Mailbox)(implicit F: Async[F]) {
  import IOMailbox._

  /** Accepts `a` unless the mailbox is closed. */
  def offerSystem(a: A): F[Boolean] = {
    state.modify[(Boolean, F[Unit])] { s =>
      if (s.closed) (s, (false, F.unit))
      else (s.copy(next = s.next + 1, system = s.system.enqueue((s.next, a)), waiters = noWaiters), (true, wake(s)))
    }.flatMap { case (accepted, w) => w.as(accepted) }
  }

  /** Offers `a` under the configured limit. */
  def offer(a: A): F[Offer[A]] = {
    state.modify[(Offer[A], F[Unit])] { s =>
      if (s.closed) {
        (s, (Offer.Closed, F.unit))
      } else {
        config match {
          case Mailbox.Bounded(capacity, overflow) if s.userSize >= capacity =>
            val first = !s.overflowing
            overflow match {
              case Mailbox.Overflow.DropNew =>
                (s.copy(overflowing = true), (Offer.Dropped(a, first), F.unit))
              case Mailbox.Overflow.Fail =>
                (s.copy(overflowing = true), (Offer.Rejected(first), F.unit))
              case Mailbox.Overflow.DropOldest =>
                val ((_, oldest), rest) = s.user.dequeue
                val next = s.copy(next = s.next + 1, user = rest.enqueue((s.next, a)), overflowing = true, waiters = noWaiters)
                (next, (Offer.Dropped(oldest, first), wake(s)))
            }
          case _ =>
            (s.copy(next = s.next + 1, user = s.user.enqueue((s.next, a)), userSize = s.userSize + 1, waiters = noWaiters), (Offer.Accepted, wake(s)))
        }
      }
    }.flatMap { case (result, w) => w.as(result) }
  }

  /** Offers `a` and turns the result into an effect: fails if the mailbox is closed or the policy is [[Mailbox.Overflow.Fail]],
    * and logs a warning when an overflow starts (not on every dropped message).
    */
  def send(a: A, path: String, log: Logger[F]): F[Unit] = {
    offer(a).flatMap {
      case Offer.Accepted         => F.unit
      case Offer.Closed           => F.raiseError(new Exception(s"Actor $path already stopped"))
      case Offer.Dropped(_, true) => log.warn(s"Mailbox of $path is full (${describe(config)}), dropping messages")
      case Offer.Dropped(_, _)    => F.unit
      case Offer.Rejected(first) =>
        val error = MailboxOverflow(path, capacity(config))
        (if (first) log.warn(error.getMessage) else F.unit) >> F.raiseError(error)
    }
  }

  /** Waits for the next element. `None` once the mailbox is closed and empty. */
  def take: F[Option[A]] = {
    Deferred[F, Unit].flatMap { waiter =>
      state.modify { s =>
        pop(s) match {
          case Some((a, next))  => (next, F.pure(Option(a)))
          case None if s.closed => (s, F.pure(Option.empty[A]))
          case None             => (s.copy(waiters = waiter :: s.waiters), waiter.get >> take)
        }
      }.flatten
    }
  }

  def stream: Stream[F, A] = Stream.repeatEval(take).unNoneTerminate

  def close: F[Unit] = state.modify(s => (s.copy(closed = true, waiters = noWaiters), wake(s))).flatten

  def size: F[Int] = state.get.map(s => s.userSize + s.system.size)

  private def pop(s: State[F, A]): Option[(A, State[F, A])] = {
    (s.system.headOption, s.user.headOption) match {
      case (Some((i, a)), Some((j, _))) if i < j => Some((a, s.copy(system = s.system.tail)))
      case (Some((_, a)), None)                  => Some((a, s.copy(system = s.system.tail)))
      case (_, Some((_, a))) =>
        val size = s.userSize - 1
        Some((a, s.copy(user = s.user.tail, userSize = size, overflowing = s.overflowing && size >= capacity(config))))
      case (None, None) => None
    }
  }

  private val noWaiters: List[Deferred[F, Unit]] = Nil

  private def wake(s: State[F, A]): F[Unit] = s.waiters.traverse_(_.complete(()))

}

private[actor] object IOMailbox {

  /** `userSize` is tracked apart because the length of an immutable queue is linear. `overflowing` is set by the first
    * message over the limit and cleared once the queue has room again, so each overflow is logged once.
    */
  final case class State[F[_], A](
      next: Long,
      system: Queue[(Long, A)],
      user: Queue[(Long, A)],
      userSize: Int,
      overflowing: Boolean,
      closed: Boolean,
      waiters: List[Deferred[F, Unit]]
  )

  sealed trait Offer[+A]

  object Offer {
    case object Accepted extends Offer[Nothing]
    case object Closed extends Offer[Nothing]

    /** `message` is the one discarded: the new one with DropNew, the oldest one with DropOldest. */
    final case class Dropped[A](message: A, first: Boolean) extends Offer[A]
    final case class Rejected(first: Boolean) extends Offer[Nothing]
  }

  def apply[F[_], A](config: Mailbox)(implicit F: Async[F]): F[IOMailbox[F, A]] = {
    Ref.of[F, State[F, A]](State(0L, Queue.empty, Queue.empty, 0, overflowing = false, closed = false, Nil)).map(IOMailbox(_, config))
  }

  private def capacity(config: Mailbox): Int = config match {
    case Mailbox.Bounded(capacity, _) => capacity
    case Mailbox.Unbounded            => Int.MaxValue
  }

  private def describe(config: Mailbox): String = config match {
    case Mailbox.Bounded(capacity, overflow) => s"capacity $capacity, $overflow"
    case Mailbox.Unbounded                   => "unbounded"
  }

}
