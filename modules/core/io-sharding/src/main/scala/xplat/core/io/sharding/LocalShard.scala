package xplat.core.io.sharding

import cats.effect.kernel.{Async, Clock, Deferred, Ref}
import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all._
import cats.implicits._
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.error.MailboxOverflow
import xplat.core.api.{ActorRef, Context}
import xplat.core.sharding.api.{Entity, EntityContext, EntityTypeKey}

import scala.concurrent.duration._

/** A shard hosted on this node: the entities of one shard id, as children of a holder actor.
  *
  * Entities start on their first message and are named after their id. A passivated entity keeps the messages that
  * arrive while it stops, and starts again with them once it is gone.
  */
private[sharding] final class LocalShard[F[_], T](
    entity: Entity[F, T],
    val shardId: String,
    holder: Context[F, Unit],
    stopHolder: F[Unit],
    entities: Ref[F, Map[String, LocalShard.EntityState[F, T]]],
    lock: Semaphore[F],
    closed: Deferred[F, Unit],
    supervisor: Supervisor[F],
    log: Log4Cats[F]
)(implicit A: Async[F]) {
  import LocalShard._

  private val typeKey: EntityTypeKey[T] = entity.typeKey

  def entityIds: F[Set[String]] = entities.get.map(_.keySet)

  def deliver(entityId: String, msg: T): F[Unit] =
    lock.permit.surround {
      Clock[F].monotonic.flatMap { now =>
        entities.get.map(_.get(entityId)).flatMap {
          case Some(Active(ref, _)) =>
            entities.update(_.updated(entityId, Active(ref, now))) >>
              ref.send(msg).recoverWith {
                // A full mailbox means the entity is alive: waiting for it to stop would block the whole shard.
                case error: MailboxOverflow => A.raiseError(error)
                case _                      => ref.terminated >> spawn(entityId).flatMap(_.send(msg))
              }
          case Some(Passivating(ref, buffered)) => entities.update(_.updated(entityId, Passivating(ref, buffered :+ msg)))
          case None                             => spawn(entityId).flatMap(_.send(msg))
        }
      }
    }

  /** Starts entities without a message, e.g. remembered entities after the shard moved. */
  def start(ids: Set[String]): F[Unit] =
    lock.permit.surround(entities.get.flatMap(current => (ids -- current.keySet).toList.traverse_(spawn)))

  def passivate(entityId: String): F[Unit] =
    lock.permit.surround {
      entities.get.map(_.get(entityId)).flatMap {
        case Some(Active(ref, _)) => entities.update(_.updated(entityId, Passivating(ref, Vector.empty))) >> stop(entityId, ref)
        case _                    => A.unit
      }
    }

  /** Must be called with the lock held. */
  private def spawn(entityId: String): F[ActorRef[F, T]] =
    for {
      now <- Clock[F].monotonic
      ref <- holder.spawn[T](entityId, entity.mailbox)(b => entity.behavior(context(entityId))(b))
      _   <- entities.update(_.updated(entityId, Active(ref, now)))
      _   <- supervisor.supervise(ref.terminated >> onTerminated(entityId, ref))
    } yield ref

  private def onTerminated(entityId: String, ref: ActorRef[F, T]): F[Unit] =
    lock.permit.surround {
      entities.get.map(_.get(entityId)).flatMap {
        case Some(Active(r, _)) if r == ref => entities.update(_ - entityId)
        case Some(Passivating(r, buffered)) if r == ref =>
          if (buffered.isEmpty) entities.update(_ - entityId)
          else closed.tryGet.flatMap {
            case Some(_) => entities.update(_ - entityId)
            case None    => spawn(entityId).flatMap(next => buffered.traverse_(m => next.send(m).recoverWith(dropOnOverflow(entityId))))
          }
        case _ => A.unit
      }
    }

  /** Messages buffered during passivation have no sender waiting for them, so an overflow drops them instead of
    * stopping the replay.
    */
  private def dropOnOverflow(entityId: String): PartialFunction[Throwable, F[Unit]] = { case error: MailboxOverflow =>
    log.warn(s"Entity ${typeKey.name}/$entityId: buffered message dropped, ${error.getMessage}")
  }

  private def stop(entityId: String, ref: ActorRef[F, T]): F[Unit] =
    entity.stopMessage.fold(holder.stopChild(entityId))(m => ref.send(m).attempt.void)

  private def context(id: String): EntityContext[F, T] = new EntityContext[F, T] {
    val typeKey: EntityTypeKey[T] = entity.typeKey
    val entityId: String = id
    val shardId: String = LocalShard.this.shardId
    def passivate: F[Unit] = LocalShard.this.passivate(id)
  }

  /** Passivates entities that received no message for `passivateIdleAfter`. */
  private def idleLoop(idle: FiniteDuration): F[Unit] =
    (A.sleep((idle / 2).max(10.millis)) >> Clock[F].monotonic.flatMap { now =>
      entities.get.flatMap(_.toList.traverse_ {
        case (id, Active(_, lastUsed)) if now - lastUsed >= idle => passivate(id)
        case _                                                  => A.unit
      })
    }).foreverM.race(closed.get).void

  def startIdleLoop: F[Unit] = entity.config.passivateIdleAfter.traverse_(idle => supervisor.supervise(idleLoop(idle))).void

  /** Stops every entity, waiting up to `handOffTimeout`, then the holder. Returns the ids that were running. */
  def stopAll: F[Set[String]] =
    closed.complete(()) >> lock.permit.surround(entities.get).flatMap { current =>
      val refs = current.toList.map {
        case (id, Active(ref, _))         => (id, ref)
        case (id, Passivating(ref, _))    => (id, ref)
      }
      log.debug(s"Shard ${typeKey.name}/$shardId stopping ${refs.size} entities") >>
        refs.traverse_ { case (id, ref) => stop(id, ref) } >>
        refs.traverse_(_._2.terminated).timeoutTo(entity.config.handOffTimeout, refs.traverse_ { case (id, _) => holder.stopChild(id) } >> refs.traverse_(_._2.terminated)) >>
        stopHolder.as(current.keySet)
    }
}

private[sharding] object LocalShard {
  sealed trait EntityState[F[_], T]
  final case class Active[F[_], T](ref: ActorRef[F, T], lastUsed: FiniteDuration) extends EntityState[F, T]
  final case class Passivating[F[_], T](ref: ActorRef[F, T], buffered: Vector[T]) extends EntityState[F, T]

  /** Creates the shard with its holder actor under `guardian`. */
  def apply[F[_], T](entity: Entity[F, T], shardId: String, guardian: Context[F, Unit], supervisor: Supervisor[F], log: Log4Cats[F])(implicit A: Async[F]): F[LocalShard[F, T]] = {
    val holderName = s"${entity.typeKey.name}-$shardId"
    for {
      holderCtx <- Deferred[F, Context[F, Unit]]
      holderRef <- guardian.spawn[Unit](holderName)(b => b.context(ctx => holderCtx.complete(ctx).as(b.same)))
      ctx       <- holderCtx.get
      entities  <- Ref.of[F, Map[String, EntityState[F, T]]](Map.empty)
      lock      <- Semaphore[F](1)
      closed    <- Deferred[F, Unit]
      shard = new LocalShard[F, T](entity, shardId, ctx, guardian.stopChild(holderName) >> holderRef.terminated, entities, lock, closed, supervisor, log)
      _ <- shard.startIdleLoop
    } yield shard
  }
}
