package xplat.core.io.sharding

import cats.effect.kernel.{Async, Clock}
import cats.implicits._
import xplat.core.api.{Behavior, Behaviors, Context}
import xplat.core.cluster.api.{Address, MemberStatus}
import xplat.core.io.cluster.IOCluster
import xplat.core.io.sharding.ShardingProtocol._
import xplat.core.sharding.api.{ShardAllocationStrategy, ShardingConfig}

import scala.concurrent.duration.FiniteDuration

/** Decides where the shards of one entity type live. Runs as a cluster singleton.
  *
  * Its allocations and remembered entities are kept in a [[CoordinatorStore]], so they survive the coordinator moving
  * or the whole cluster restarting. Regions also report their shards every `retryInterval`, which corrects the stored
  * view: during the first two intervals after starting it does not allocate new shards, and a stored allocation that its
  * region does not report is treated as lost, so the shard is placed again and its remembered entities restart.
  */
private[sharding] object Coordinator {

  final case class State(
      allocations: Map[String, Address],
      remembered: Map[String, Set[String]],
      registered: Set[Address],
      rebalancing: Set[String],
      waiting: Map[String, Set[Address]],
      startedAt: FiniteDuration,
      lastRebalance: FiniteDuration,
      reported: Map[Address, Set[String]],
      allocatedAt: Map[String, FiniteDuration]
  ) {
    def shardsOf(region: Address): Set[String] = allocations.collect { case (s, r) if r == region => s }.toSet
    def data: CoordinatorStore.Data = CoordinatorStore.Data(allocations, remembered)
  }

  def behavior[F[_]](
      cluster: IOCluster[F],
      config: ShardingConfig,
      strategy: ShardAllocationStrategy[F],
      regionEndpoint: String,
      remember: Boolean,
      store: CoordinatorStore[F]
  )(b: Behaviors[F, CoordinatorMsg])(implicit A: Async[F]): Behavior[F, CoordinatorMsg] = {

    def tell(region: Address, msg: RegionMsg): F[Unit] =
      A.fromEither(regionCodec.encode(msg).leftMap(e => new IllegalStateException(e.message))).flatMap(cluster.remoting.send(region, regionEndpoint, _))

    def statuses: F[Map[Address, (MemberStatus, Boolean)]] =
      cluster.state.map(_.members.map(m => m.address -> (m.status, config.role.forall(m.hasRole))).toMap)

    def alive(s: Map[Address, (MemberStatus, Boolean)], a: Address): Boolean =
      s.get(a).exists { case (status, _) => status != MemberStatus.Down && status != MemberStatus.Removed }

    def eligible(st: State, s: Map[Address, (MemberStatus, Boolean)]): Set[Address] =
      st.registered.filter(a => s.get(a).exists { case (status, hasRole) => status == MemberStatus.Up && hasRole })

    /** Places `shard` and tells its new home and the regions waiting for it. */
    def allocate(st: State, shard: String, requesters: Set[Address], exclude: Set[Address]): F[State] =
      statuses.flatMap { s =>
        val all = eligible(st, s)
        val candidates = if ((all -- exclude).nonEmpty) all -- exclude else all
        if (candidates.isEmpty) A.pure(st.copy(waiting = st.waiting.updated(shard, st.waiting.getOrElse(shard, Set.empty) ++ requesters)))
        else {
          val requester = requesters.headOption.getOrElse(candidates.min)
          (strategy.allocate(requester, shard, candidates.map(a => a -> st.shardsOf(a)).toMap), Clock[F].monotonic).tupled.flatMap { case (home, now) =>
            val entities = if (remember) st.remembered.getOrElse(shard, Set.empty) else Set.empty[String]
            (requesters + home).toList.traverse_(r => tell(r, ShardHome(shard, home, if (r == home) entities else Set.empty))).as(
              st.copy(allocations = st.allocations.updated(shard, home), waiting = st.waiting - shard, allocatedAt = st.allocatedAt.updated(shard, now))
            )
          }
        }
      }

    /** Shards that lost their home: placed again at once if they have remembered entities to restart. */
    def reallocateRemembered(st: State, shards: Iterable[String], exclude: Set[Address]): F[State] =
      shards.toList.foldM(st) { (acc, shard) =>
        if (remember && acc.remembered.getOrElse(shard, Set.empty).nonEmpty) allocate(acc, shard, Set.empty, exclude) else A.pure(acc)
      }

    def handle(ctx: Context[F, CoordinatorMsg], st: State, msg: CoordinatorMsg): F[State] = msg match {

      case Register(region, shards) =>
        shards.toList.foldM(st.copy(registered = st.registered + region, reported = st.reported.updated(region, shards.keySet))) { case (acc, (shard, entities)) =>
          acc.allocations.get(shard) match {
            case None => A.pure(acc.copy(allocations = acc.allocations.updated(shard, region), remembered = acc.remembered.updated(shard, entities)))
            case Some(home) if home == region => A.pure(acc.copy(remembered = acc.remembered.updated(shard, entities)))
            // Two regions claim the shard: the duplicate stops it.
            case Some(_) => tell(region, HandOff(shard)).as(acc)
          }
        }

      case GetShardHome(shard, requester) =>
        if (st.rebalancing(shard)) A.pure(st.copy(waiting = st.waiting.updated(shard, st.waiting.getOrElse(shard, Set.empty) + requester)))
        else
          statuses.flatMap { s =>
            st.allocations.get(shard) match {
              case Some(home) if alive(s, home) => tell(requester, ShardHome(shard, home, Set.empty)).as(st)
              case _ =>
                Clock[F].monotonic.flatMap { now =>
                  val graceOver = now - st.startedAt >= config.retryInterval * 2
                  if (graceOver) allocate(st.copy(allocations = st.allocations - shard), shard, Set(requester), Set.empty) else A.pure(st)
                }
            }
          }

      case ShardStopped(shard, region, entities) =>
        if (!st.allocations.get(shard).contains(region)) A.pure(st.copy(rebalancing = st.rebalancing - shard))
        else {
          val freed = st.copy(
            allocations = st.allocations - shard,
            rebalancing = st.rebalancing - shard,
            remembered = if (remember) st.remembered.updated(shard, entities) else st.remembered
          )
          st.waiting.get(shard) match {
            case Some(waiters) if waiters.nonEmpty => allocate(freed, shard, waiters, Set(region))
            case _                                 => reallocateRemembered(freed, Seq(shard), Set(region))
          }
        }

      case RegionStopped(region) =>
        val lost = st.shardsOf(region)
        reallocateRemembered(
          st.copy(registered = st.registered - region, reported = st.reported - region, allocations = st.allocations -- lost, rebalancing = st.rebalancing -- lost),
          lost,
          Set(region)
        )

      case Tick =>
        for {
          _   <- ctx.schedule(ctx.pipe(Tick), config.retryInterval)
          now <- Clock[F].monotonic
          s   <- statuses
          // Shards of regions on nodes that are gone lose their home.
          deadShards = st.allocations.collect { case (shard, home) if !alive(s, home) => shard }.toSet
          cleaned = st.copy(
            allocations = st.allocations -- deadShards,
            rebalancing = st.rebalancing -- deadShards,
            registered = st.registered.filter(alive(s, _))
          )
          afterDead0 <- reallocateRemembered(cleaned, deadShards, Set.empty)
          // Allocations its live region does not report (e.g. loaded from the store after a restart) are lost shards.
          settled = config.retryInterval * 3
          orphans = afterDead0.allocations.collect {
            case (shard, home)
                if now - afterDead0.startedAt >= settled && !afterDead0.rebalancing(shard) &&
                  afterDead0.reported.get(home).exists(!_.contains(shard)) && afterDead0.allocatedAt.get(shard).forall(now - _ >= settled) =>
              shard
          }.toSet
          afterOrphans <- reallocateRemembered(afterDead0.copy(allocations = afterDead0.allocations -- orphans), orphans, Set.empty)
          // Remembered entities whose shard has no home (e.g. after the whole cluster restarted) start again.
          unplaced = afterOrphans.remembered.collect {
            case (shard, ids) if ids.nonEmpty && !afterOrphans.allocations.contains(shard) && !afterOrphans.rebalancing(shard) => shard
          }
          afterDead <- if (now - afterOrphans.startedAt >= settled) reallocateRemembered(afterOrphans, unplaced, Set.empty) else A.pure(afterOrphans)
          // Shards of regions on leaving nodes move away before the node goes.
          leavingShards = afterDead.allocations.collect {
            case (shard, home) if !afterDead.rebalancing(shard) && s.get(home).exists(x => x._1 == MemberStatus.Leaving || x._1 == MemberStatus.Exiting) => shard -> home
          }
          _ <- leavingShards.toList.traverse_ { case (shard, home) => tell(home, HandOff(shard)) }
          afterLeaving = afterDead.copy(rebalancing = afterDead.rebalancing ++ leavingShards.keySet)
          result <-
            if (now - afterLeaving.lastRebalance < config.rebalanceInterval) A.pure(afterLeaving)
            else {
              val regions = eligible(afterLeaving, s)
              strategy.rebalance(regions.map(a => a -> afterLeaving.shardsOf(a)).toMap, afterLeaving.rebalancing).flatMap { shards =>
                shards.toList.traverse_(shard => afterLeaving.allocations.get(shard).traverse_(tell(_, HandOff(shard)))).as(
                  afterLeaving.copy(rebalancing = afterLeaving.rebalancing ++ shards, lastRebalance = now)
                )
              }
            }
        } yield result
    }

    def running(ctx: Context[F, CoordinatorMsg], st: State): Behavior[F, CoordinatorMsg] =
      b.message(msg => handle(ctx, st, msg).flatTap(next => store.save(st.data, next.data)).map(next => running(ctx, next)))

    b.context { ctx =>
      for {
        now  <- Clock[F].monotonic
        data <- store.load
        _    <- ctx.schedule(ctx.pipe(Tick), config.retryInterval)
      } yield running(
        ctx,
        State(data.allocations, data.remembered, Set.empty, Set.empty, Map.empty, now, now, Map.empty, data.allocations.map { case (s, _) => s -> now })
      )
    }
  }
}
