package xplat.core.io.cluster

import cats.effect.kernel.{Async, Clock, Deferred, Ref, Resource}
import cats.effect.std.{Dispatcher, Queue, Semaphore, Supervisor}
import cats.effect.syntax.all._
import cats.implicits._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.{ActorSystem, Context}
import xplat.core.cluster.Cluster
import xplat.core.cluster.api.SplitBrainStrategy.{LeaseMajority, LocalStrategy}
import xplat.core.cluster.api._
import xplat.core.coordination.{Lease, LeaseProvider, LeaseSettings}
import xplat.core.io.cluster.internal._
import xplat.core.io.cluster.transport.{Transport, TransportFactory}

import scala.concurrent.duration._

/** Cluster node over a [[TransportFactory]].
  *
  * Membership spreads by gossip: every `gossipInterval` the node sends its view to all members, and views are merged by
  * version. A node is unreachable when no heartbeat arrived from it for `unreachableAfter`. The leader (lowest reachable
  * address) moves members through their statuses once every reachable member has seen the same view.
  */
final class IOCluster[F[_]] private (
    val system: ActorSystem[F],
    val config: ClusterConfig,
    val self: UniqueAddress,
    val remoting: Remoting[F],
    val refResolver: RefResolver[F],
    val asks: Asks[F],
    val receptionist: IOReceptionist[F],
    val leaseProvider: Option[LeaseProvider[F]],
    sendWire: (Address, Wire) => F[Unit],
    gossip: Ref[F, GossipState],
    mutex: Semaphore[F],
    subscribers: Ref[F, Map[Long, Queue[F, ClusterEvent]]],
    nextSubscriber: Ref[F, Long],
    lastSeen: Ref[F, Map[UniqueAddress, FiniteDuration]],
    joined: Deferred[F, Unit],
    up: Deferred[F, Unit],
    removed: Deferred[F, Unit],
    guardians: Ref[F, Map[String, Deferred[F, Context[F, Unit]]]],
    sbr: Ref[F, IOCluster.SbrState],
    sbrLease: Option[Lease[F]],
    supervisor: Supervisor[F],
    log: Log4Cats[F]
)(implicit A: Async[F])
    extends Cluster[F] {

  // ---- Cluster API ----

  def selfMember: F[Member] =
    gossip.get.map(_.members.getOrElse(self, Member(self, MemberStatus.Removed, config.roles)))

  def state: F[ClusterState] = gossip.get.map(_.view)

  def events: fs2.Stream[F, ClusterEvent] =
    fs2.Stream
      .bracket(
        mutex.permit.surround(
          for {
            id    <- nextSubscriber.getAndUpdate(_ + 1)
            queue <- Queue.unbounded[F, ClusterEvent]
            _     <- subscribers.update(_.updated(id, queue))
            g     <- gossip.get
          } yield (id, queue, g)
        )
      ) { case (id, _, _) => subscribers.update(_ - id) }
      .flatMap { case (_, queue, g) => fs2.Stream.emits(Events.initial(g.view)) ++ fs2.Stream.fromQueueUnterminated(queue) }

  def join(seedNodes: List[Address]): F[Unit] = startJoining(seedNodes) >> joined.get

  def awaitUp: F[Unit] = up.get

  def leave(address: Address): F[Unit] =
    update { g =>
      g.changed(self) { g0 =>
        g0.members.values
          .filter(m => m.address == address && Set[MemberStatus](MemberStatus.Joining, MemberStatus.WeaklyUp, MemberStatus.Up)(m.status))
          .foldLeft(g0)((acc, m) => acc.withStatus(m.uniqueAddress, MemberStatus.Leaving))
      }
    }.void

  def down(address: Address): F[Unit] =
    update(g => g.changed(self)(g0 => g0.members.values.filter(_.address == address).foldLeft(g0)((acc, m) => acc.withStatus(m.uniqueAddress, MemberStatus.Down)))).void

  /** A node that was removed or downed takes no further part in the cluster. */
  def isTerminated: F[Boolean] = removed.tryGet.map(_.isDefined)

  def awaitTerminated: F[Unit] = removed.get

  /** Actor under which cluster tools (singletons, shards) spawn their actors. One per name, created on first use. */
  def guardian(name: String): F[Context[F, Unit]] =
    Deferred[F, Context[F, Unit]].flatMap { fresh =>
      guardians.modify { current =>
        current.get(name) match {
          case Some(existing) => (current, existing.get)
          case None =>
            val create = system.spawn[Unit](s"cluster-$name")(b => b.context(ctx => fresh.complete(ctx).as(b.same))) >> fresh.get
            (current.updated(name, fresh), create)
        }
      }.flatten
    }

  // ---- State changes ----

  /** Applies `f` to the gossip and publishes what changed. Serialized, so events come out in order. */
  private def update(f: GossipState => GossipState): F[GossipState] =
    mutex.permit.surround {
      gossip.get.flatMap { before =>
        val after = f(before)
        if (after == before) A.pure(after)
        else gossip.set(after) >> afterChange(before, after).as(after)
      }
    }

  private def afterChange(before: GossipState, after: GossipState): F[Unit] = {
    val events = Events.diff(before.view, after.view)
    for {
      now <- Clock[F].monotonic
      _   <- lastSeen.update(seen => after.members.keys.foldLeft(seen)((acc, id) => if (acc.contains(id)) acc else acc.updated(id, now)) -- after.tombstones)
      subs <- subscribers.get
      _   <- subs.values.toList.traverse_(q => events.traverse_(q.offer))
      _   <- A.whenA(after.members.contains(self))(joined.complete(()).void)
      _   <- A.whenA(after.members.get(self).exists(m => Set[MemberStatus](MemberStatus.Up, MemberStatus.Leaving, MemberStatus.Exiting)(m.status)))(up.complete(()).void)
      _   <- A.whenA(after.tombstones(self) || after.members.get(self).exists(_.status == MemberStatus.Down))(removed.complete(()).void)
      _   <- receptionist.membershipChanged(after)
      _   <- events.traverse_(e => log.debug(s"$self: $e"))
    } yield ()
  }

  // ---- Incoming messages ----

  def receive(bytes: Array[Byte]): F[Unit] =
    A.delay(Wire.decode(bytes)).attempt.flatMap {
      case Left(e)                      => log.warn(e)("Dropping a message that cannot be decoded")
      case Right(Wire.Join(node, roles)) => handleJoin(node, roles)
      case Right(Wire.Gossip(from, g))  => handleGossip(from, g)
      case Right(Wire.Heartbeat(from))  => Clock[F].monotonic.flatMap(now => lastSeen.update(_.updated(from, now)))
      case Right(r: Wire.Registry)      => receptionist.receive(r)
      case Right(other)                 => remoting.receive(other)
    }

  private def handleJoin(node: UniqueAddress, roles: Set[String]): F[Unit] =
    isTerminated.ifM(
      A.unit,
      update { g =>
        if (!g.members.contains(self) || g.tombstones(node) || g.members.contains(node)) g
        else
          g.changed(self) { g0 =>
            // A node restarted on the same address replaces its previous incarnation.
            val previous = g0.members.values.filter(m => m.address == node.address && m.uniqueAddress != node)
            previous.foldLeft(g0)((acc, m) => acc.withStatus(m.uniqueAddress, MemberStatus.Down)).withMember(Member(node, MemberStatus.Joining, roles))
          }
      }.flatMap(g => A.whenA(g.members.contains(node))(sendGossip(node.address, g)))
    )

  private def handleGossip(from: UniqueAddress, remote: GossipState): F[Unit] =
    isTerminated.ifM(
      A.unit,
      update { local =>
        // Only gossip from our own cluster: it knows us, or we know the sender.
        val ours = remote.members.contains(self) || remote.tombstones(self) || local.members.contains(from)
        if (ours) local.receive(self, remote) else local
      }.void
    )

  private def sendGossip(to: Address, g: GossipState): F[Unit] = sendWire(to, Wire.Gossip(self, g))

  // ---- Joining ----

  private def startJoining(seeds: List[Address]): F[Unit] = {
    val others = seeds.filter(_ != self.address)
    if (seeds.isEmpty) A.raiseError(new IllegalArgumentException("no seed nodes given"))
    else if (others.isEmpty) selfJoin
    else {
      // The first seed starts a new cluster if no other seed answers in time.
      val fallback = if (seeds.head == self.address) Some(config.retryInterval * 5) else None
      Clock[F].monotonic.flatMap(start => supervisor.supervise(joinLoop(others, start, fallback)).void)
    }
  }

  private def selfJoin: F[Unit] =
    update(g => if (g.members.isEmpty && !g.tombstones(self)) g.changed(self)(_.withMember(Member(self, MemberStatus.Joining, config.roles))) else g).void

  private def joinLoop(seeds: List[Address], start: FiniteDuration, selfJoinAfter: Option[FiniteDuration]): F[Unit] =
    joined.tryGet.flatMap {
      case Some(_) => A.unit
      case None =>
        Clock[F].monotonic.flatMap { now =>
          if (selfJoinAfter.exists(now - start >= _)) selfJoin
          else seeds.traverse_(seed => sendWire(seed, Wire.Join(self, config.roles))) >> A.sleep(config.retryInterval) >> joinLoop(seeds, start, selfJoinAfter)
        }
    }

  // ---- Periodic work ----

  private def every(interval: FiniteDuration)(tick: F[Unit]): F[Unit] =
    (tick.handleErrorWith(e => log.warn(e)("Cluster task failed")) >> A.sleep(interval)).foreverM.race(removed.get).void

  private def gossipTick: F[Unit] =
    for {
      before <- gossip.get
      after  <- update(g => if (g.members.contains(self)) g.leaderActions(self) else g)
      // Removed members are no longer gossiped to, so they get the gossip that removes them directly.
      newlyRemoved = after.tombstones -- before.tombstones - self
      _ <- newlyRemoved.toList.traverse_(id => sendGossip(id.address, after))
      _ <- A.whenA(after.members.contains(self))(after.members.keys.filter(_ != self).toList.traverse_(id => sendGossip(id.address, after)))
      _ <- receptionist.broadcast
    } yield ()

  private def heartbeatTick: F[Unit] =
    for {
      g <- gossip.get
      others = g.members.keys.filter(_ != self).toList
      _    <- others.traverse_(id => sendWire(id.address, Wire.Heartbeat(self)))
      now  <- Clock[F].monotonic
      seen <- lastSeen.get
      unreachableNow = others.filter(id => now - seen.getOrElse(id, now) > config.unreachableAfter).toSet
      _ <- update(g => if (g.members.contains(self)) g.changed(self)(_.withObservation(self, unreachableNow)) else g)
    } yield ()

  /** Split brain resolver. Only the reachable member with the lowest address decides for its side, once the view has been
    * stable for `stableAfter`.
    */
  private def sbrTick(settings: SplitBrainResolverConfig): F[Unit] =
    for {
      g   <- gossip.get
      now <- Clock[F].monotonic
      unreachableLive = g.unreachable.filter(id => g.members.get(id).exists(_.status != MemberStatus.Down))
      signature = (g.members.values.map(m => (m.uniqueAddress, m.status)).toSet, g.unreachable)
      st <- sbr.updateAndGet { st =>
        val moved = if (st.signature != signature) st.copy(signature = signature, lastChange = now) else st
        if (unreachableLive.isEmpty) moved.copy(unstableSince = None) else moved.copy(unstableSince = moved.unstableSince.orElse(Some(now)))
      }
      decider = g.reachableMembers.filter(_.status != MemberStatus.Down).map(_.uniqueAddress).minOption.contains(self)
      stable = now - st.lastChange >= settings.stableAfter
      tooLong = settings.downAllWhenUnstable.exists(limit => st.unstableSince.exists(since => now - since >= settings.stableAfter + limit))
      _ <-
        if (!g.members.contains(self) || unreachableLive.isEmpty || !decider) A.unit
        else if (stable) resolve(settings.strategy, g.view) >> sbr.update(_.copy(lastChange = now))
        else if (tooLong) downMembers(Decision.DownAll, g.view, "the cluster stayed unstable for too long")
        else A.unit
    } yield ()

  private def resolve(strategy: SplitBrainStrategy, view: ClusterState): F[Unit] =
    (strategy match {
      case local: LocalStrategy => A.pure(local.decide(view))
      case lease: LeaseMajority =>
        A.sleep(lease.acquireDelay(view)) >> sbrLease.fold(A.pure(false))(_.acquire.handleError(_ => false)).map(lease.decide(view, _))
    }).flatMap(decision => downMembers(decision, view, s"split brain resolver ($strategy)"))

  private def downMembers(decision: Decision, view: ClusterState, reason: String): F[Unit] = {
    val ids = decision.membersToDown(view).map(_.uniqueAddress)
    log.warn(s"$self: $decision because of $reason: downing ${ids.map(_.address).mkString(", ")}") >>
      update(g => g.changed(self)(g0 => ids.foldLeft(g0)((acc, id) => acc.withStatus(id, MemberStatus.Down)))).void
  }

  private[cluster] def start: F[Unit] =
    supervisor.supervise(every(config.gossipInterval)(gossipTick)) >>
      supervisor.supervise(every(config.heartbeatInterval)(heartbeatTick)) >>
      config.splitBrainResolver.traverse_(settings => supervisor.supervise(every(config.heartbeatInterval)(sbrTick(settings)))) >>
      A.whenA(config.seedNodes.nonEmpty)(startJoining(config.seedNodes))

  /** Leaves the cluster and waits until the others have removed this node. */
  private[cluster] def gracefulLeave: F[Unit] =
    isTerminated.ifM(
      A.unit,
      gossip.get.flatMap { g =>
        A.whenA(g.members.contains(self)) {
          val timeout = (config.gossipInterval * 30).max(2.seconds)
          leave(self.address) >> removed.get.timeout(timeout).attempt.void
        }
      }
    )
}

object IOCluster {

  private[cluster] final case class SbrState(
      signature: (Set[(UniqueAddress, MemberStatus)], Set[UniqueAddress]),
      lastChange: FiniteDuration,
      unstableSince: Option[FiniteDuration]
  )

  /** Builder of IO cluster nodes.
    *
    * @param transport
    *   [[transport.TcpTransport]] for real deployments, [[transport.InMemoryNetwork]] for tests.
    * @param leaseProvider
    *   needed by the `LeaseMajority` split brain strategy and by singletons or shards that use a lease.
    */
  def builder[F[_]](transport: TransportFactory[F], leaseProvider: Option[LeaseProvider[F]] = None)(implicit A: Async[F]): ClusterBuilder[F] =
    (system: ActorSystem[F], config: ClusterConfig) => resource(system, config, transport, leaseProvider)

  /** The IO implementation behind a [[Cluster]], to reach what cluster tools need. */
  def unwrap[F[_]](cluster: Cluster[F])(implicit A: Async[F]): F[IOCluster[F]] = cluster match {
    case io: IOCluster[F] @unchecked => A.pure(io)
    case other                       => A.raiseError(new IllegalArgumentException(s"$other is not an IO cluster"))
  }

  private def resource[F[_]](system: ActorSystem[F], config: ClusterConfig, factory: TransportFactory[F], leaseProvider: Option[LeaseProvider[F]])(implicit
      A: Async[F]
  ): Resource[F, Cluster[F]] =
    for {
      log        <- Resource.eval(Slf4jLogger.fromName[F]("xplat.cluster"))
      supervisor <- Supervisor[F](await = false)
      dispatcher <- Dispatcher.parallel[F](await = false)
      uid        <- Resource.eval(A.delay(scala.util.Random.nextLong()))
      instance   <- Resource.eval(Deferred[F, IOCluster[F]])
      transport  <- factory.bind(system.name, config.host, config.port, bytes => instance.get.flatMap(_.receive(bytes)))
      self = UniqueAddress(transport.address, uid)
      sendWire = (to: Address, wire: Wire) => transport.send(to, Wire.encode(wire)).attempt.void
      remoting <- Resource.eval(Remoting[F](self.address, sendWire, supervisor, log))
      resolver = new IORefResolver[F](remoting, dispatcher, log)
      asks = new Asks[F](remoting, resolver)
      gossip       <- Resource.eval(Ref.of[F, GossipState](GossipState.empty))
      receptionist <- Resource.eval(IOReceptionist[F](self, remoting, asks, sendWire, gossip, supervisor))
      sbrLease     <- splitBrainLease(config, self, leaseProvider)
      now          <- Resource.eval(Clock[F].monotonic)
      cluster <- Resource.eval(
        (
          Semaphore[F](1),
          Ref.of[F, Map[Long, Queue[F, ClusterEvent]]](Map.empty),
          Ref.of[F, Long](0L),
          Ref.of[F, Map[UniqueAddress, FiniteDuration]](Map.empty),
          Deferred[F, Unit],
          Deferred[F, Unit],
          Deferred[F, Unit],
          Ref.of[F, Map[String, Deferred[F, Context[F, Unit]]]](Map.empty),
          Ref.of[F, SbrState](SbrState((Set.empty, Set.empty), now, None))
        ).mapN { (mutex, subscribers, nextSubscriber, lastSeen, joined, up, removed, guardians, sbr) =>
          new IOCluster[F](
            system, config, self, remoting, resolver, asks, receptionist, leaseProvider, sendWire, gossip, mutex, subscribers, nextSubscriber, lastSeen, joined, up,
            removed, guardians, sbr, sbrLease, supervisor, log
          )
        }
      )
      _ <- Resource.eval(instance.complete(cluster))
      _ <- Resource.eval(cluster.start)
      _ <- Resource.onFinalize(cluster.gracefulLeave)
    } yield cluster

  private def splitBrainLease[F[_]](config: ClusterConfig, self: UniqueAddress, provider: Option[LeaseProvider[F]])(implicit
      A: Async[F]
  ): Resource[F, Option[Lease[F]]] =
    config.splitBrainResolver.map(_.strategy) match {
      case Some(strategy: LeaseMajority) =>
        provider match {
          case Some(p) => p.lease(LeaseSettings(strategy.leaseName, self.address.toString)).map(Some(_))
          case None    => Resource.eval(A.raiseError(new IllegalArgumentException("the LeaseMajority strategy needs a LeaseProvider")))
        }
      case _ => Resource.pure(None)
    }
}
