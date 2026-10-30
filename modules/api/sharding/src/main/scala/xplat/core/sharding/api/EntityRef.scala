package xplat.core.sharding.api

import xplat.core.cluster.api.ClusterRef

/** Reference to a sharded entity, wherever it runs. Sending starts the entity if it is not running. */
trait EntityRef[F[_], T] extends ClusterRef[F, T] {
  def typeKey: EntityTypeKey[T]
  def entityId: String
}
