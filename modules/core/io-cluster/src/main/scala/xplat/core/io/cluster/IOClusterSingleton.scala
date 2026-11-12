package xplat.core.io.cluster

import cats.effect.kernel.{Async, Deferred, Ref, Resource}
import cats.effect.std.{Semaphore, Supervisor}
import cats.effect.syntax.all._
import cats.implicits._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.error.XError
import xplat.core.api.{ActorRef, Codec, ReplyTo}
import xplat.core.cluster.api._
import xplat.core.cluster.{Cluster, ClusterSingleton}
import xplat.core.coordination.{Lease, LeaseSettings}
import xplat.core.io.cluster.internal.Asks

import scala.concurrent.duration._

/** Cluster singletons on an [[IOCluster]].
  *
  * Each node runs a manager per singleton. The manager on the oldest Up member (with the role) starts the singleton, but
  * only after every other member confirmed it is not running it, so two copies never overlap: an unreachable member
  * blocks the start until it is downed. Messages sent while the singleton moves are buffered.
  */
final class IOClusterSingleton[F[_]] private (
    cluster: IOCluster[F],
    managers: Ref[F, Map[String, Deferred[F, IOClusterSingleton.Manager[F, _]]]],
    supervisor: Supervisor[F],
    log: Log4Cats[F]
)(implicit A: Async[F])
    extends ClusterSingleton[F] {
  import IOClusterSingleton._

  /** The first `init` of a name creates its manager; later ones return the same proxy. */
  def init[T](singleton: SingletonActor[F, T]): F[ClusterRef[F, T]] =
    Deferred[F, Manager[F, _]].flatMap { fresh =>
      managers.modify { current =>
        current.get(singleton.name) match {
          case Some(existing) => (current, existing.get)
          case None =>
            val create = Manager[F, T](cluster, singleton, supervisor, log).flatTap(m => m.start >> fresh.complete(m)).widen[Manager[F, _]]
            (current.updated(singleton.name, fresh), create)
        }
      }.flatten.map(_.asInstanceOf[Manager[F, T]].proxy)
    }

  private def stopAll: F[Unit] = managers.get.flatMap(_.values.toList.traverse_(_.tryGet.flatMap(_.traverse_(_.shutdown))))
}

object IOClusterSingleton {

  /** Builder for `ClusterSingleton(cluster)`. The cluster must be an [[IOCluster]]. */
  def builder[F[_]](implicit A: Async[F]): ClusterSingletonBuilder[F] =
    (cluster: Cluster[F]) =>
      for {
        io         <- Resource.eval(IOCluster.unwrap(cluster))
        log        <- Resource.eval(Slf4jLogger.fromName[F]("xplat.cluster.singleton"))
        supervisor <- Supervisor[F](await = false)
        managers   <- Resource.eval(Ref.of[F, Map[String, Deferred[F, Manager[F, _]]]](Map.empty))
        singletons = new IOClusterSingleton[F](io, managers, supervisor, log)
        // Stopping the local singletons lets them start on the next oldest member.
        _ <- Resource.onFinalize(singletons.stopAll)
      } yield singletons

  private[cluster] sealed trait Phase[F[_], T]
  private[cluster] final case class Idle[F[_], T]() extends Phase[F, T]
  private[cluster] final case class Running[F[_], T](ref: ActorRef[F, T], lease: Option[Lease[F]]) extends Phase[F, T]

  /** The singleton stopped by itself; it is not started again while this node stays the oldest. */
  private[cluster] final case class Finished[F[_], T]() extends Phase[F, T]

  private[cluster] final class Manager[F[_], T](
      cluster: IOCluster[F],
      singleton: SingletonActor[F, T],
      phase: Ref[F, Phase[F, T]],
      transition: Semaphore[F],
      buffer: Ref[F, Vector[Array[Byte]]],
      leases: Ref[F, Option[(Lease[F], F[Unit])]],
      stopped: Deferred[F, Unit],
      supervisor: Supervisor[F],
      log: Log4Cats[F]
  )(implicit A: Async[F]) {

    private val config = singleton.config
    private val endpoint = s"/singleton/${singleton.name}"
    private val forwardedEndpoint = s"$endpoint/forwarded"
    private val handOverEndpoint = s"$endpoint/hand-over"
    private val self = cluster.self.address
    private val codec: Codec[T, Array[Byte]] = singleton.codec

    /** The oldest Up member with the role: where the singleton must run. */
    private def oldest: F[Option[Address]] =
      cluster.state.map { s =>
        s.members.filter(m => m.status == MemberStatus.Up && config.role.forall(m.hasRole)).minOption(Member.ageOrdering).map(_.address)
      }

    // ---- Messages ----

    private def deliverLocal(ref: ActorRef[F, T], bytes: Array[Byte]): F[Unit] =
      A.fromEither(codec.decode(bytes).leftMap(e => XError(e.message))).flatMap(ref.send).handleErrorWith(e => log.warn(e)(s"Singleton ${singleton.name}: message dropped"))

    private def bufferMessage(bytes: Array[Byte]): F[Unit] =
      buffer.update(b => (b :+ bytes).takeRight(config.bufferSize))

    /** Message for the singleton that reached this node. If the singleton is not here, it goes where it should be. */
    private def onMessage(bytes: Array[Byte], forwarded: Boolean): F[Unit] =
      phase.get.flatMap {
        case Running(ref, _) => deliverLocal(ref, bytes)
        case _ =>
          oldest.flatMap {
            case Some(target) if target != self && !forwarded => cluster.remoting.send(target, forwardedEndpoint, bytes)
            case _                                            => bufferMessage(bytes)
          }
      }

    private def flushBuffer: F[Unit] =
      buffer.getAndSet(Vector.empty).flatMap(_.traverse_(bytes => onMessage(bytes, forwarded = false)))

    val proxy: ClusterRef[F, T] = new ClusterRef[F, T] {
      val path: String = s"singleton://${singleton.name}"

      def send(msg: T): F[Unit] =
        A.fromEither(codec.encode(msg).leftMap(e => XError(e.message))).flatMap { bytes =>
          oldest.flatMap {
            case Some(target) => cluster.remoting.send(target, endpoint, bytes)
            case None         => bufferMessage(bytes)
          }
        }

      def ask[B](f: ReplyTo[B] => T, timeout: FiniteDuration)(implicit replyCodec: Codec[B, Array[Byte]]): F[B] =
        new Asks[F](cluster.remoting, cluster.refResolver).ask[T, B](send, f, timeout)
    }

    // ---- Start and stop ----

    private def leaseSettings: Option[LeaseSettings] =
      config.lease.map(u => LeaseSettings(s"${cluster.system.name}-singleton-${singleton.name}", self.toString, u.timeToLive, u.heartbeatInterval))

    private def acquireLease: F[Option[Option[Lease[F]]]] =
      (leaseSettings, cluster.leaseProvider) match {
        case (None, _) => A.pure(Some(None))
        case (Some(_), None) =>
          log.error(s"Singleton ${singleton.name} needs a lease but the cluster has no LeaseProvider").as(None)
        case (Some(settings), Some(provider)) =>
          leases.get.flatMap {
            case Some((lease, _)) => A.pure(lease)
            case None             => provider.lease(settings).allocated.flatTap(l => leases.set(Some(l))).map(_._1)
          }.flatMap(lease => lease.acquire.handleError(_ => false).map(ok => if (ok) Some(Some(lease)) else None))
      }

    /** Asks every other member to confirm it is not running the singleton. */
    private def othersStopped: F[Boolean] =
      cluster.state.flatMap { s =>
        val others = s.members.filter(m => m.address != self && m.status != MemberStatus.Down).map(_.address).toList
        val timeout = config.handOverRetryInterval.max(500.millis)
        others.traverse(addr => cluster.remoting.request(addr, handOverEndpoint, Array.emptyByteArray, timeout).attempt.map(_.isRight)).map(_.forall(identity))
      }

    private def tryStart: F[Unit] =
      othersStopped.ifM(
        acquireLease.flatMap {
          case None => A.unit
          case Some(lease) =>
            for {
              guardian <- cluster.guardian("singletons")
              ref      <- guardian.spawn[T](singleton.name)(singleton.behavior)
              _        <- phase.set(Running(ref, lease))
              _        <- log.info(s"Singleton ${singleton.name} started on $self")
              _        <- supervisor.supervise(ref.terminated >> phase.update { case Running(r, _) if r == ref => Finished(); case other => other })
              _        <- lease.traverse_(l => supervisor.supervise(l.lost >> log.warn(s"Singleton ${singleton.name} lost its lease") >> transition.permit.surround(stopLocal)))
              _        <- flushBuffer
            } yield ()
        },
        A.unit
      )

    /** Stops the local singleton, if running, and waits until it is gone. */
    private def stopLocal: F[Unit] =
      phase.get.flatMap {
        case Running(ref, lease) =>
          val ask = singleton.stopMessage.fold(cluster.guardian("singletons").flatMap(_.stopChild(singleton.name)))(m => ref.send(m).attempt.void)
          val force = cluster.guardian("singletons").flatMap(_.stopChild(singleton.name))
          log.info(s"Singleton ${singleton.name} stopping on $self") >>
            ask >> ref.terminated.timeoutTo(10.seconds, force >> ref.terminated) >>
            lease.traverse_(_.release.attempt) >> phase.set(Idle())
        case Finished() => phase.set(Idle())
        case Idle()     => A.unit
      }

    private def tick: F[Unit] =
      cluster.isTerminated.ifM(
        transition.permit.surround(stopLocal),
        oldest.flatMap { target =>
          transition.permit.surround {
            phase.get.flatMap {
              case Idle() if target.contains(self)        => tryStart
              case Running(_, _) if !target.contains(self) => stopLocal >> flushBuffer
              case _                                      => A.whenA(target.exists(_ != self))(flushBuffer)
            }
          }
        }
      )

    def start: F[Unit] =
      cluster.remoting.on(endpoint)(onMessage(_, forwarded = false)) >>
        cluster.remoting.on(forwardedEndpoint)(onMessage(_, forwarded = true)) >>
        cluster.remoting.onRequest(handOverEndpoint)(_ => transition.permit.surround(stopLocal).as(Array.emptyByteArray)) >>
        supervisor.supervise((tick.handleErrorWith(e => log.warn(e)(s"Singleton ${singleton.name} manager failed")) >> A.sleep(config.handOverRetryInterval)).foreverM.race(stopped.get)).void

    def shutdown: F[Unit] =
      stopped.complete(()) >> transition.permit.surround(stopLocal) >>
        cluster.remoting.off(endpoint) >> cluster.remoting.off(forwardedEndpoint) >> cluster.remoting.offRequest(handOverEndpoint) >>
        leases.getAndSet(None).flatMap(_.traverse_(_._2))
  }

  private[cluster] object Manager {
    def apply[F[_], T](cluster: IOCluster[F], singleton: SingletonActor[F, T], supervisor: Supervisor[F], log: Log4Cats[F])(implicit A: Async[F]): F[Manager[F, T]] =
      (
        Ref.of[F, Phase[F, T]](Idle()),
        Semaphore[F](1),
        Ref.of[F, Vector[Array[Byte]]](Vector.empty),
        Ref.of[F, Option[(Lease[F], F[Unit])]](None),
        Deferred[F, Unit]
      ).mapN(new Manager[F, T](cluster, singleton, _, _, _, _, _, supervisor, log))
  }
}
