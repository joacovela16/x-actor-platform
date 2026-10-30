package xplat.core.io.sharding

import cats.effect.kernel.{Async, Deferred, Ref, Resource}
import cats.effect.std.Supervisor
import cats.implicits._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.{ActorRef, Codec, Ref => ReplyTo}
import xplat.core.cluster.ClusterSingleton
import xplat.core.cluster.api.{SingletonActor, SingletonConfig}
import xplat.core.io.cluster.internal.Asks
import xplat.core.io.cluster.{IOCluster, IOClusterSingleton}
import xplat.core.io.sharding.ShardingProtocol._
import xplat.core.sharding.ClusterSharding
import xplat.core.sharding.api._
import xplat.core.sharding.api.error.EntityTypeNotInitialized

import scala.concurrent.duration.FiniteDuration

/** Cluster sharding on an [[IOCluster]]. Each entity type has a region on every node that called `init`, and a
  * coordinator that runs as a cluster singleton.
  */
final class IOClusterSharding[F[_]] private (
    cluster: IOCluster[F],
    singletons: ClusterSingleton[F],
    regions: Ref[F, Map[String, Deferred[F, Region[F, _]]]],
    supervisor: Supervisor[F],
    log: Log4Cats[F]
)(implicit A: Async[F])
    extends ClusterSharding[F] {

  private val asks = new Asks[F](cluster.remoting, cluster.refResolver)

  def init[T](entity: Entity[F, T]): F[ActorRef[F, ShardingEnvelope[T]]] =
    Deferred[F, Region[F, _]].flatMap { fresh =>
      regions.modify { current =>
        current.get(entity.typeKey.name) match {
          case Some(existing) => (current, existing.get)
          case None           => (current.updated(entity.typeKey.name, fresh), create(entity).flatTap(fresh.complete).widen[Region[F, _]])
        }
      }.flatten.map(region => envelopeRef(entity.typeKey, region.asInstanceOf[Region[F, T]]))
    }

  private def create[T](entity: Entity[F, T]): F[Region[F, T]] = {
    val config = entity.config
    val coordinator = SingletonActor[F, CoordinatorMsg](
      s"sharding-coordinator-${entity.typeKey.name}",
      b => Coordinator.behavior[F](cluster, config, entity.allocationStrategy.getOrElse(new LeastShardAllocationStrategy[F]()), Region.endpoint(entity.typeKey.name), config.rememberEntities)(b),
      SingletonConfig(role = config.role, handOverRetryInterval = config.retryInterval)
    )(coordinatorCodec)
    for {
      proxy  <- singletons.init(coordinator)
      region <- Region[F, T](cluster, entity, proxy, supervisor, log)
      _      <- region.start
    } yield region
  }

  private def region[T](typeKey: EntityTypeKey[T]): F[Region[F, T]] =
    regions.get.flatMap(_.get(typeKey.name) match {
      case Some(d) => d.get.map(_.asInstanceOf[Region[F, T]])
      case None    => A.raiseError(EntityTypeNotInitialized(typeKey.name))
    })

  def entityRefFor[T](typeKey: EntityTypeKey[T], entityId: String): EntityRef[F, T] =
    IOEntityRef[F, T](typeKey, entityId)(region(typeKey), asks)

  def regionState[T](typeKey: EntityTypeKey[T]): F[ShardRegionState] = region(typeKey).flatMap(_.state)

  /** Ref that accepts envelopes for any entity of the type. Replies need a codec, so use `entityRefFor(...).ask`. */
  private def envelopeRef[T](typeKey: EntityTypeKey[T], region: Region[F, T]): ActorRef[F, ShardingEnvelope[T]] =
    new ActorRef[F, ShardingEnvelope[T]] {
      val name: String = s"${typeKey.name}-region"
      val path: String = s"sharding://${typeKey.name}"
      def send(msg: ShardingEnvelope[T]): F[Unit] = region.deliver(msg.entityId, msg.message)
      def ask[B](f: ReplyTo[B] => ShardingEnvelope[T], timeout: FiniteDuration): F[B] =
        A.raiseError(new UnsupportedOperationException("the shard region cannot carry replies; use entityRefFor(...).ask"))
      def terminated: F[Unit] = A.never
    }

  private def stopAll: F[Unit] =
    regions.get.flatMap(_.values.toList.traverse_(_.tryGet.flatMap(_.traverse_(_.shutdown))))
}

object IOClusterSharding {

  /** Builder for `ClusterSharding(cluster)`. The cluster must be an [[IOCluster]]. Releasing it hands off the local
    * shards to other nodes.
    */
  def builder[F[_]](implicit A: Async[F]): ClusterShardingBuilder[F] =
    cluster =>
      for {
        io         <- Resource.eval(IOCluster.unwrap(cluster))
        log        <- Resource.eval(Slf4jLogger.fromName[F]("xplat.sharding"))
        singletons <- IOClusterSingleton.builder[F].build(cluster)
        supervisor <- Supervisor[F](await = false)
        regions    <- Resource.eval(Ref.of[F, Map[String, Deferred[F, Region[F, _]]]](Map.empty))
        sharding = new IOClusterSharding[F](io, singletons, regions, supervisor, log)
        _ <- Resource.onFinalize(sharding.stopAll)
      } yield sharding
}

/** Reference to one entity. It looks up the local region on every send, so it can be created before `init`. */
private[sharding] final case class IOEntityRef[F[_], T](typeKey: EntityTypeKey[T], entityId: String)(region: F[Region[F, T]], asks: Asks[F])(implicit A: Async[F])
    extends EntityRef[F, T] {

  val path: String = s"sharding://${typeKey.name}/$entityId"

  def send(msg: T): F[Unit] = region.flatMap(_.deliver(entityId, msg))

  def ask[B](f: ReplyTo[B] => T, timeout: FiniteDuration)(implicit codec: Codec[B, Array[Byte]]): F[B] =
    asks.ask[T, B](send, f, timeout)
}
