package xplat.core.io.sharding

import cats.effect.kernel.{Async, Clock, Deferred, Ref}
import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all._
import cats.implicits._
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.error.XError
import xplat.core.cluster.api.{Address, ClusterRef, MemberStatus}
import xplat.core.coordination.{Lease, LeaseSettings}
import xplat.core.io.cluster.IOCluster
import xplat.core.io.sharding.ShardingProtocol._
import xplat.core.sharding.api.{Entity, ShardRegionState}

import scala.concurrent.duration._

/** Shard region of one entity type on this node.
  *
  * It routes every message to the region that hosts the entity's shard, asking the coordinator where that is the first
  * time. Messages wait in a buffer while the home is unknown or the shard is moving. A region on a node without the
  * configured role only routes; it never hosts shards.
  */
private[sharding] final class Region[F[_], T](
    cluster: IOCluster[F],
    entity: Entity[F, T],
    coordinator: ClusterRef[F, CoordinatorMsg],
    val canHost: Boolean,
    homes: Ref[F, Map[String, Address]],
    buffers: Ref[F, Map[String, Vector[(String, T)]]],
    lastRequest: Ref[F, Map[String, FiniteDuration]],
    shards: Ref[F, Map[String, Region.Slot[F, T]]],
    lock: Semaphore[F],
    stopped: Deferred[F, Unit],
    supervisor: Supervisor[F],
    log: Log4Cats[F]
)(implicit A: Async[F]) {
  import Region._

  private val config = entity.config
  private val self = cluster.self.address
  val endpoint: String = Region.endpoint(entity.typeKey.name)

  private def shardOf(entityId: String): String = entity.shardIdExtractor.shardId(entityId)

  // ---- Routing ----

  def deliver(entityId: String, msg: T): F[Unit] = lock.permit.surround(route(shardOf(entityId), entityId, msg))

  /** Must be called with the lock held. */
  private def route(shard: String, entityId: String, msg: T): F[Unit] =
    homes.get.map(_.get(shard)).flatMap {
      case Some(home) if home == self =>
        shards.get.map(_.get(shard)).flatMap {
          case Some(Started(local, _)) => local.deliver(entityId, msg)
          case _                       => buffer(shard, entityId, msg)
        }
      case Some(home) =>
        A.fromEither(entity.codec.encode(msg).leftMap(e => XError(e.message))).flatMap { bytes =>
          A.fromEither(regionCodec.encode(EntityMessage(entityId, bytes)).leftMap(e => XError(e.message))).flatMap(cluster.remoting.send(home, endpoint, _))
        }
      case None => buffer(shard, entityId, msg) >> requestHome(shard)
    }

  private def buffer(shard: String, entityId: String, msg: T): F[Unit] =
    buffers.update { b =>
      val queued = b.getOrElse(shard, Vector.empty) :+ (entityId -> msg)
      b.updated(shard, queued.takeRight(MaxBuffered))
    }

  private def requestHome(shard: String): F[Unit] =
    Clock[F].monotonic.flatMap { now =>
      lastRequest.modify { last =>
        if (last.get(shard).exists(now - _ < config.retryInterval)) (last, false) else (last.updated(shard, now), true)
      }.ifM((coordinator ! GetShardHome(shard, self)).attempt.void, A.unit)
    }

  /** Must be called with the lock held. */
  private def flush(shard: String): F[Unit] =
    buffers.modify(b => (b - shard, b.getOrElse(shard, Vector.empty))).flatMap(_.traverse_ { case (id, msg) => route(shard, id, msg) })

  // ---- Messages from the coordinator and other regions ----

  def onMessage(bytes: Array[Byte]): F[Unit] =
    A.fromEither(regionCodec.decode(bytes).leftMap(e => XError(e.message))).flatMap {
      case ShardHome(shard, home, entities) => onShardHome(shard, home, entities)
      case HandOff(shard)                   => handOff(shard, reportTo = true)
      case EntityMessage(entityId, payload) =>
        A.fromEither(entity.codec.decode(payload).leftMap(e => XError(e.message))).flatMap { msg =>
          val shard = shardOf(entityId)
          lock.permit.surround {
            homes.get.map(_.get(shard)).flatMap {
              case Some(home) if home == self => route(shard, entityId, msg)
              // The sender's view was stale: find out where the shard lives now.
              case _ => homes.update(_ - shard) >> buffer(shard, entityId, msg) >> requestHome(shard)
            }
          }
        }
    }

  private def onShardHome(shard: String, home: Address, entities: Set[String]): F[Unit] =
    lock.permit.surround {
      homes.update(_.updated(shard, home)) >> {
        if (home == self) startShard(shard, entities)
        else
          shards.get.map(_.get(shard)).flatMap {
            case Some(_) => log.warn(s"Shard ${entity.typeKey.name}/$shard now lives on $home; stopping the local copy") >> supervisor.supervise(handOff(shard, reportTo = false)).void
            case None    => A.unit
          }
      } >> flush(shard)
    }

  // ---- Local shards ----

  /** Must be called with the lock held. Starts the shard in the background, since acquiring its lease may take time. */
  private def startShard(shard: String, entities: Set[String]): F[Unit] =
    shards.get.map(_.get(shard)).flatMap {
      case Some(Started(local, _)) => local.start(entities)
      case Some(Starting())        => A.unit
      case None if !canHost        => log.warn(s"Shard ${entity.typeKey.name}/$shard was placed on $self, which lacks the role ${config.role.getOrElse("")}")
      case None                    => shards.update(_.updated(shard, Starting())) >> supervisor.supervise(doStart(shard, entities)).void
    }

  private def leaseSettings(shard: String): Option[LeaseSettings] =
    config.lease.map(u => LeaseSettings(s"${cluster.system.name}-shard-${entity.typeKey.name}-$shard", self.toString, u.timeToLive, u.heartbeatInterval))

  private def acquireLease(shard: String): F[Option[Option[(Lease[F], F[Unit])]]] =
    (leaseSettings(shard), cluster.leaseProvider) match {
      case (None, _)    => A.pure(Some(None))
      case (Some(_), None) => log.error(s"Sharding of ${entity.typeKey.name} needs a lease but the cluster has no LeaseProvider").as(None)
      case (Some(settings), Some(provider)) =>
        provider.lease(settings).allocated.flatMap { case (lease, release) =>
          lease.acquire.handleError(_ => false).flatMap(ok => if (ok) A.pure(Some(Some((lease, release)))) else release.as(None))
        }
    }

  private def doStart(shard: String, entities: Set[String]): F[Unit] =
    acquireLease(shard).flatMap {
      case None => lock.permit.surround(shards.update(_ - shard)) // retried by the maintenance loop
      case Some(lease) =>
        for {
          guardian <- cluster.guardian("sharding")
          local    <- LocalShard[F, T](entity, shard, guardian, supervisor, log)
          _ <- lock.permit.surround {
            shards.get.map(_.get(shard)).flatMap {
              case Some(Starting()) =>
                shards.update(_.updated(shard, Started(local, lease.map(_._2)))) >> local.start(entities) >> flush(shard)
              // Handed off while starting: undo.
              case _ => supervisor.supervise(local.stopAll >> lease.traverse_(_._2)).void
            }
          }
          _ <- lease.traverse_ { case (l, _) => supervisor.supervise(l.lost >> log.warn(s"Shard ${entity.typeKey.name}/$shard lost its lease") >> handOff(shard, reportTo = true)) }
        } yield ()
    }

  /** Stops a local shard and, if asked, tells the coordinator so it can place it again. */
  private def handOff(shard: String, reportTo: Boolean): F[Unit] =
    lock.permit
      .surround(homes.update(_ - shard) >> shards.modify(s => (s - shard, s.get(shard))))
      .flatMap {
        case Some(Started(local, releaseLease)) => local.stopAll.flatTap(_ => releaseLease.sequence_)
        case _                                  => A.pure(Set.empty[String])
      }
      .flatMap(entities => A.whenA(reportTo)((coordinator ! ShardStopped(shard, self, entities)).attempt.void))

  def hosted: F[Map[String, Set[String]]] =
    shards.get.flatMap(_.toList.traverse {
      case (shard, Started(local, _)) => local.entityIds.map(ids => Option(shard -> ids))
      case _                          => A.pure(Option.empty[(String, Set[String])])
    }).map(_.flatten.toMap)

  def state: F[ShardRegionState] = hosted.map(ShardRegionState(_))

  // ---- Maintenance ----

  private def maintenance: F[Unit] =
    for {
      members <- cluster.state.map(_.members.map(m => m.address -> m.status).toMap)
      _       <- A.whenA(canHost)(hosted.flatMap(h => (coordinator ! Register(self, h)).attempt.void))
      _ <- lock.permit.surround {
        for {
          // Homes on nodes that are gone are forgotten, so their shards are asked for again.
          _       <- homes.update(_.filter { case (_, home) => members.get(home).exists(s => s != MemberStatus.Down && s != MemberStatus.Removed) })
          current <- homes.get
          slots   <- shards.get
          _       <- current.toList.traverse_ { case (shard, home) => A.whenA(home == self && !slots.contains(shard))(startShard(shard, Set.empty)) }
          pending <- buffers.get
          _ <- pending.keys.toList.traverse_(shard => if (current.contains(shard)) flush(shard) else requestHome(shard))
        } yield ()
      }
    } yield ()

  def start: F[Unit] =
    cluster.remoting.on(endpoint)(onMessage) >>
      supervisor.supervise((maintenance.handleErrorWith(e => log.warn(e)("Shard region maintenance failed")) >> A.sleep(config.retryInterval)).foreverM.race(stopped.get)).void

  /** Hands off every local shard and tells the coordinator this region is gone. */
  def shutdown: F[Unit] =
    stopped.complete(()) >>
      shards.get.flatMap(_.keys.toList.traverse_(handOff(_, reportTo = true))) >>
      (coordinator ! RegionStopped(self)).attempt.void >>
      cluster.remoting.off(endpoint)
}

private[sharding] object Region {

  /** Messages kept per shard while its home is unknown. Beyond it the oldest are dropped. */
  val MaxBuffered: Int = 100000

  def endpoint(typeName: String): String = s"/sharding/$typeName/region"

  sealed trait Slot[F[_], T]
  final case class Starting[F[_], T]() extends Slot[F, T]
  final case class Started[F[_], T](local: LocalShard[F, T], releaseLease: Option[F[Unit]]) extends Slot[F, T]

  def apply[F[_], T](cluster: IOCluster[F], entity: Entity[F, T], coordinator: ClusterRef[F, CoordinatorMsg], supervisor: Supervisor[F], log: Log4Cats[F])(implicit
      A: Async[F]
  ): F[Region[F, T]] = {
    val canHost = entity.config.role.forall(cluster.config.roles.contains)
    (
      Ref.of[F, Map[String, Address]](Map.empty),
      Ref.of[F, Map[String, Vector[(String, T)]]](Map.empty),
      Ref.of[F, Map[String, FiniteDuration]](Map.empty),
      Ref.of[F, Map[String, Slot[F, T]]](Map.empty),
      Semaphore[F](1),
      Deferred[F, Unit]
    ).mapN(new Region[F, T](cluster, entity, coordinator, canHost, _, _, _, _, _, _, supervisor, log))
  }
}
