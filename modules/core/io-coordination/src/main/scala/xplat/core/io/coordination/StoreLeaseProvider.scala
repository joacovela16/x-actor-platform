package xplat.core.io.coordination

import cats.effect.kernel.{Async, Clock, Deferred, Fiber, Ref, Resource}
import cats.effect.std.Supervisor
import cats.effect.syntax.all._
import cats.implicits._
import xplat.core.coordination.{Lease, LeaseProvider, LeaseSettings}

import scala.concurrent.duration._

/** Leases backed by a [[LeaseStore]]. A held lease is renewed every `heartbeatInterval`. It is lost when the store says
  * someone else holds it, or when renewals keep failing (store unreachable) for `timeToLive`, because by then the store
  * may have given it to someone else.
  */
final class StoreLeaseProvider[F[_]](store: LeaseStore[F])(implicit A: Async[F]) extends LeaseProvider[F] {

  def lease(settings: LeaseSettings): Resource[F, Lease[F]] =
    for {
      supervisor <- Supervisor[F](await = false)
      held       <- Resource.eval(Ref.of[F, Option[Fiber[F, Throwable, Unit]]](None))
      lostSignal <- Resource.eval(Deferred[F, Unit].flatMap(Ref.of[F, Deferred[F, Unit]]))
      lease = new StoreLease[F](settings, store, supervisor, held, lostSignal)
      _ <- Resource.onFinalize(lease.release.attempt.void)
    } yield lease
}

private final class StoreLease[F[_]](
    val settings: LeaseSettings,
    store: LeaseStore[F],
    supervisor: Supervisor[F],
    held: Ref[F, Option[Fiber[F, Throwable, Unit]]],
    lostSignal: Ref[F, Deferred[F, Unit]]
)(implicit A: Async[F])
    extends Lease[F] {

  private def call[B](op: F[B]): F[B] = op.timeout(settings.operationTimeout)

  def acquire: F[Boolean] = held.get.flatMap {
    case Some(_) => A.pure(true)
    case None =>
      call(store.tryAcquire(settings.name, settings.owner, settings.timeToLive)).flatTap { acquired =>
        A.whenA(acquired)(Clock[F].monotonic.flatMap(at => supervisor.supervise(renewLoop(at))).flatMap(fiber => held.set(Some(fiber))))
      }
  }

  private def renewLoop(lastRenewed: FiniteDuration): F[Unit] =
    A.sleep(settings.heartbeatInterval) >> call(store.renew(settings.name, settings.owner, settings.timeToLive)).attempt.flatMap {
      case Right(true) => Clock[F].monotonic.flatMap(renewLoop)
      case Right(false) => markLost
      case Left(_) =>
        Clock[F].monotonic.flatMap { now =>
          if (now - lastRenewed >= settings.timeToLive) markLost else renewLoop(lastRenewed)
        }
    }

  /** Runs on the renewal fiber itself, so it must not cancel that fiber. */
  private def markLost: F[Unit] =
    held.set(None) >> Deferred[F, Unit].flatMap(next => lostSignal.getAndSet(next)).flatMap(_.complete(())).void

  def release: F[Boolean] = held.getAndSet(None).flatMap {
    case None        => A.pure(false)
    case Some(fiber) => fiber.cancel >> call(store.release(settings.name, settings.owner))
  }

  def checkLease: F[Boolean] = held.get.map(_.isDefined)

  def lost: F[Unit] = lostSignal.get.flatMap(_.get)
}

object StoreLeaseProvider {
  def apply[F[_]: Async](store: LeaseStore[F]): LeaseProvider[F] = new StoreLeaseProvider[F](store)

  /** Provider with its own in-memory store. Leases are only exclusive within this process. */
  def inMemory[F[_]: Async]: F[LeaseProvider[F]] = InMemoryLeaseStore[F].map(apply[F](_))
}
