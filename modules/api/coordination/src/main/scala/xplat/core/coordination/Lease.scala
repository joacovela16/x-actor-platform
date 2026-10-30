package xplat.core.coordination

import cats.effect.Resource

/** Exclusive permission with an expiry, kept by a store outside the cluster (Kubernetes, etcd, ZooKeeper, a database).
  *
  * At most one owner holds a lease at a time. The owner renews it every `heartbeatInterval`; if it stops (crash, network
  * cut), the lease expires after `timeToLive` and someone else can take it. Because the store sits outside the cluster, it
  * can settle conflicts that cluster members cannot settle among themselves, like which side of a split survives.
  *
  * {{{
  * Lease[IO](LeaseSettings("billing-writer", owner = selfAddress.toString)).use { lease =>
  *   lease.acquire.ifM(
  *     IO.race(writeInvoices, lease.lost).void, // stop writing as soon as the lease is lost
  *     IO.sleep(retry)
  *   )
  * }
  * }}}
  */
trait Lease[F[_]] {

  def settings: LeaseSettings

  /** Tries to take the lease and keeps renewing it in the background. Returns `false` if another owner holds it. Taking a
    * lease the owner already holds succeeds.
    */
  def acquire: F[Boolean]

  /** Gives the lease back so another owner can take it right away. Returns `false` if it was not held. */
  def release: F[Boolean]

  /** Whether this owner holds the lease, according to the last renewal. Cheap: it does not call the store. */
  def checkLease: F[Boolean]

  /** Completes when a held lease is lost, for example after renewals failed for `timeToLive`. The owner must stop the
    * protected work, because someone else may take the lease.
    */
  def lost: F[Unit]
}

/** Connects leases to a concrete store. */
trait LeaseProvider[F[_]] {

  /** Releasing the resource stops renewals and releases the lease if held. */
  def lease(settings: LeaseSettings): Resource[F, Lease[F]]
}

object Lease {
  def apply[F[_]](settings: LeaseSettings)(implicit provider: LeaseProvider[F]): Resource[F, Lease[F]] = provider.lease(settings)
}
