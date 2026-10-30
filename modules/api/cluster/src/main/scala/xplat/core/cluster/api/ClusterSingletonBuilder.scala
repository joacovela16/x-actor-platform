package xplat.core.cluster.api

import cats.effect.Resource
import xplat.core.cluster.{Cluster, ClusterSingleton}

trait ClusterSingletonBuilder[F[_]] {
  def build(cluster: Cluster[F]): Resource[F, ClusterSingleton[F]]
}
