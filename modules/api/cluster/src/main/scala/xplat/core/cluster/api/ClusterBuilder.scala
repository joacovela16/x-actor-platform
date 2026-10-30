package xplat.core.cluster.api

import cats.effect.Resource
import xplat.core.api.ActorSystem
import xplat.core.cluster.Cluster

trait ClusterBuilder[F[_]] {
  def build(system: ActorSystem[F], config: ClusterConfig): Resource[F, Cluster[F]]
}
