package xplat.core.sharding.api

import cats.effect.Resource
import xplat.core.cluster.Cluster
import xplat.core.sharding.ClusterSharding

trait ClusterShardingBuilder[F[_]] {
  def build(cluster: Cluster[F]): Resource[F, ClusterSharding[F]]
}
