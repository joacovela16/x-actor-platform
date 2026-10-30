package xplat.core.sharding

import cats.effect.Resource
import xplat.core.cluster.Cluster
import xplat.core.cluster.api.ClusterRef
import xplat.core.sharding.api._

/** Spreads entities (actors identified by a type and an id) over the cluster, so each entity runs on exactly one node
  * and can be reached from any node without knowing where it is.
  *
  * {{{
  * val AccountKey = EntityTypeKey[AccountCmd]("account")
  *
  * Cluster(system, config).flatMap(ClusterSharding(_)).use { sharding =>
  *   for {
  *     _       <- sharding.init(Entity(AccountKey)(ctx => Persistence("account", Account.empty, onCommand, onEvent)))
  *     account  = sharding.entityRefFor(AccountKey, "acc-42")
  *     _       <- account ! Deposit(100)
  *     balance <- account ? (GetBalance(_))
  *   } yield balance
  * }
  * }}}
  */
trait ClusterSharding[F[_]] {

  /** Starts the shard region for this entity type on the local node. Call it on every node that sends to or hosts these
    * entities. The returned ref accepts envelopes for any entity, also with `ask`; calling `init` again returns the same
    * ref.
    */
  def init[T](entity: Entity[F, T]): F[ClusterRef[F, ShardingEnvelope[T]]]

  /** Reference to one entity. It is cheap and can be created per message. Sending fails with
    * [[api.error.EntityTypeNotInitialized]] if `init` was not called for this type on the local node.
    */
  def entityRefFor[T](typeKey: EntityTypeKey[T], entityId: String): EntityRef[F, T]

  /** Shards and entities of this type running on the local node. */
  def regionState[T](typeKey: EntityTypeKey[T]): F[ShardRegionState]
}

object ClusterSharding {

  /** Starts sharding on a cluster node. Releasing the resource hands off the local shards to other nodes. */
  def apply[F[_]](cluster: Cluster[F])(implicit builder: ClusterShardingBuilder[F]): Resource[F, ClusterSharding[F]] =
    builder.build(cluster)
}
