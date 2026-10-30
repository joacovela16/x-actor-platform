package xplat.core.cluster

import cats.effect.Resource
import xplat.core.cluster.api.{ClusterRef, ClusterSingletonBuilder, SingletonActor}

/** Runs actors that must exist only once in the whole cluster (a coordinator, a scheduler, a single writer).
  *
  * {{{
  * ClusterSingleton(cluster).use { singletons =>
  *   for {
  *     scheduler <- singletons.init(SingletonActor[IO, SchedulerCmd]("scheduler")(Scheduler.behavior))
  *     _         <- scheduler ! Schedule(job)
  *   } yield ()
  * }
  * }}}
  *
  * A singleton is a single point of failure and a bottleneck, and while it moves it cannot answer. Prefer sharding when
  * the work can be split.
  */
trait ClusterSingleton[F[_]] {

  /** Declares the singleton on this node and returns a reference that always reaches it, wherever it runs. Call it on
    * every node that uses or may host the singleton. Messages sent while it moves are buffered.
    */
  def init[T](singleton: SingletonActor[F, T]): F[ClusterRef[F, T]]
}

object ClusterSingleton {

  /** Releasing the resource stops the singletons running on this node so they move to another one. */
  def apply[F[_]](cluster: Cluster[F])(implicit builder: ClusterSingletonBuilder[F]): Resource[F, ClusterSingleton[F]] =
    builder.build(cluster)
}
