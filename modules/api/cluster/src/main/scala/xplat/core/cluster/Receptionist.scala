package xplat.core.cluster

import xplat.core.api.ActorRef
import xplat.core.cluster.api.{Listing, ServiceKey}

/** Cluster-wide registry of actors by [[ServiceKey]], so actors can find each other without knowing where they run.
  *
  * {{{
  * val WorkerKey = ServiceKey[Job]("worker")
  *
  * cluster.receptionist.register(WorkerKey, worker) >>
  *   cluster.receptionist.subscribe(WorkerKey).evalMap(listing => router.updateWorkers(listing.services)).compile.drain
  * }}}
  *
  * The registry is eventually consistent: a new registration reaches other nodes after a short delay.
  */
trait Receptionist[F[_]] {

  /** Registers a local actor. It is removed when it is deregistered, when the actor stops or when its node leaves. */
  def register[T](key: ServiceKey[T], ref: ActorRef[F, T]): F[Unit]

  def deregister[T](key: ServiceKey[T], ref: ActorRef[F, T]): F[Unit]

  /** Actors currently registered under the key. */
  def find[T](key: ServiceKey[T]): F[Listing[F, T]]

  /** The current listing, then a new one every time the actors registered under the key change. */
  def subscribe[T](key: ServiceKey[T]): fs2.Stream[F, Listing[F, T]]
}
