package xplat.core.io.coordination

import cats.effect.kernel.{Clock, Ref, Sync}
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

/** Store that keeps the leases: who holds each one and until when. The lease logic (renewals, loss detection) lives in
  * [[StoreLeaseProvider]]; a store only has to apply these operations atomically.
  */
trait LeaseStore[F[_]] {

  /** Takes the lease if it is free, expired or already held by `owner`. Returns whether `owner` holds it now. */
  def tryAcquire(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean]

  /** Extends the lease if `owner` still holds it (and it has not expired). Returns `false` if it was lost. */
  def renew(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean]

  /** Frees the lease if `owner` holds it. Returns whether it was held. */
  def release(name: String, owner: String): F[Boolean]

  /** Current holder, if the lease is held and not expired. */
  def holder(name: String): F[Option[String]]
}

/** Lease store kept in memory. Only shared by code running in the same process; use [[LeaseServer]] to share it with
  * other processes.
  */
final class InMemoryLeaseStore[F[_]] private (leases: Ref[F, Map[String, InMemoryLeaseStore.Entry]])(implicit F: Sync[F]) extends LeaseStore[F] {
  import InMemoryLeaseStore.Entry

  private def now: F[Long] = Clock[F].monotonic.map(_.toNanos)

  private def live(entry: Option[Entry], at: Long): Option[Entry] = entry.filter(_.expiresAt > at)

  def tryAcquire(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] = now.flatMap { at =>
    leases.modify { current =>
      live(current.get(name), at) match {
        case Some(entry) if entry.owner != owner => (current, false)
        case _                                   => (current.updated(name, Entry(owner, at + timeToLive.toNanos)), true)
      }
    }
  }

  def renew(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] = now.flatMap { at =>
    leases.modify { current =>
      live(current.get(name), at) match {
        case Some(entry) if entry.owner == owner => (current.updated(name, Entry(owner, at + timeToLive.toNanos)), true)
        case _                                   => (current, false)
      }
    }
  }

  def release(name: String, owner: String): F[Boolean] = now.flatMap { at =>
    leases.modify { current =>
      live(current.get(name), at) match {
        case Some(entry) if entry.owner == owner => (current - name, true)
        case _                                   => (current, false)
      }
    }
  }

  def holder(name: String): F[Option[String]] = now.flatMap(at => leases.get.map(current => live(current.get(name), at).map(_.owner)))
}

object InMemoryLeaseStore {
  private final case class Entry(owner: String, expiresAt: Long)

  def apply[F[_]](implicit F: Sync[F]): F[LeaseStore[F]] =
    Ref.of[F, Map[String, Entry]](Map.empty).map(new InMemoryLeaseStore[F](_))
}
