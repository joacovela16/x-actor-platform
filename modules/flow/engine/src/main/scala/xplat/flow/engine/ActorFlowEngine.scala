package xplat.flow.engine

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.implicits._
import fs2.Stream
import io.circe.Json
import xplat.core.api.{ActorRef, ActorSystem}
import xplat.core.io.actor.IOPersistence
import xplat.core.persistence.Persistence
import xplat.core.persistence.api.journal.Shard
import xplat.core.persistence.api.{Journal, JournalBuilder}
import xplat.flow.engine.InstanceActor._
import xplat.flow.engine.InstanceState.{NodeStatus, Status}
import io.circe.syntax._
import xplat.flow.model.{FlowDef, NodeCatalog, StatePath, Types}
import xplat.flow.nodes.{CoreNodes, FlowRunner, NodeRegistry, NodeType}

import java.util.UUID
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** A [[FlowEngine]] where each instance is a persistent actor of the given actor system. */
final class ActorFlowEngine[F[_]] private (
    system: ActorSystem[F],
    val store: FlowStore[F],
    journal: Journal[F],
    persistence: Persistence[F],
    extraNodes: List[NodeType[F]],
    runnerNodes: FlowRunner[F] => List[NodeType[F]],
    coreNodes: Boolean,
    metrics: SegmentMetrics[F],
    config: ActorFlowEngine.Config,
    hub: EventHub[F],
    index: InstanceIndex[F],
    redactor: Redactor,
    actors: Ref[F, Map[String, ActorRef[F, Command]]],
    mutex: Semaphore[F],
    val types: F[Types]
)(implicit F: Async[F])
    extends FlowEngine[F] {

  /** What the nodes that call other flows use: it starts an instance and, if asked to, waits for it. */
  val runner: FlowRunner[F] = new FlowRunner[F] {
    def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: FlowRunner.Caller): F[FlowRunner.Result] =
      start(flowId, revision, Json.fromFields(inputs), None, Origin.Subflow(caller.instanceId, caller.nodeId)).attempt.flatMap {
        case Left(error)       => F.pure(FlowRunner.Failed(error.getMessage))
        case Right(id) if !wait => F.pure(FlowRunner.Finished(Map.empty, Some(id)))
        case Right(id) =>
          events(id).compile.drain >> state(id).map { s =>
            if (s.status == Status.Succeeded)
              FlowRunner.Finished(s.flow.toList.flatMap(_.outputs).flatMap(o => o.target.get(s.state).map(o.name -> _)).toMap, Some(id))
            else FlowRunner.Failed(s.nodes.values.find(_.status == NodeStatus.Failed).flatMap(_.error).getOrElse("it failed"), Some(id))
          }
      }
  }

  private val registry: NodeRegistry[F] =
    (if (coreNodes) CoreNodes.registry[F](runner, CronSchedule.nextFire) else NodeRegistry[F]()) ++ new NodeRegistry[F](extraNodes ++ runnerNodes(runner))

  val catalog: NodeCatalog = registry.catalog

  // ---- actors ----

  private def spawn(id: String): F[ActorRef[F, Command]] =
    actors.get.map(_.get(id)).flatMap {
      case Some(ref) => F.pure(ref)
      case None =>
        mutex.permit.use { _ =>
          actors.get.map(_.get(id)).flatMap {
            case Some(ref) => F.pure(ref)
            case None =>
              for {
                ref <- system.spawn[Command](id)(InstanceActor.behavior(id, persistence, registry, hub, index, redactor, config.nodeTimeout, types))
                _ <- actors.update(_ + (id -> ref))
                // If the instance was running when the process stopped, its nodes go on.
                _ <- ref ! Resume
              } yield ref
          }
        }
    }

  /** Talks to the actor of the instance. If it is not there any more (it stopped), a new one is started from the journal. */
  private def withActor[A](id: String)(f: ActorRef[F, Command] => F[A]): F[A] =
    spawn(id).flatMap(f).handleErrorWith(_ => actors.update(_ - id) >> spawn(id).flatMap(f))

  private def state(id: InstanceId): F[InstanceState] = withActor(id)(_.ask[InstanceState](Get(_)))

  private def exists(id: InstanceId): F[Boolean] =
    actors.get.map(_.contains(id)).flatMap {
      case true => F.pure(true)
      case false => journal.getEvents(Shard(PersistenceName, id)).flatMap(_.take(1).compile.last).map(_.isDefined)
    }

  // ---- the interface ----

  def start(flowId: String, revision: Option[Int], inputs: Json, instanceId: Option[String], origin: Origin): F[InstanceId] =
    for {
      flow <- store.get(flowId, revision).flatMap(F.fromOption(_, FlowEngine.FlowNotFound(flowId, revision)))
      id = instanceId.getOrElse(UUID.randomUUID().toString)
      reply <- withActor(id)(_.ask[Either[String, Unit]](Start(flow, inputs, origin, _)))
      _ <- F.fromEither(reply.left.map(FlowEngine.InvalidInputs(_)))
    } yield id

  def status(id: InstanceId): F[Option[InstanceStatus]] =
    exists(id).flatMap {
      case false => F.pure(None)
      case true  => state(id).flatMap(s => metrics.snapshot(id).map(InstanceStatus.from(s, _)))
    }

  def cancel(id: InstanceId): F[Unit] =
    exists(id).flatMap {
      case false => F.raiseError[Unit](FlowEngine.InstanceNotFound(id))
      case true  => withActor(id)(_.ask[Either[String, Unit]](Cancel(_))).flatMap(r => F.fromEither(r.left.map(new IllegalStateException(_))))
    }

  def retry(id: InstanceId, useLatest: Boolean, by: Option[String]): F[Unit] =
    exists(id).flatMap {
      case false => F.raiseError[Unit](FlowEngine.InstanceNotFound(id))
      case true =>
        for {
          current <- state(id)
          flow <-
            if (useLatest) current.flow.traverse(f => store.get(f.id, None).flatMap(F.fromOption(_, FlowEngine.FlowNotFound(f.id, None))))
            else F.pure(None)
          reply <- withActor(id)(_.ask[Either[String, Unit]](Retry(flow, Origin.Retry(id, by), _)))
          _ <- F.fromEither(reply.left.map(_ => FlowEngine.NotFinished(id)))
        } yield ()
    }

  def events(id: InstanceId): Stream[F, AuditEntry] =
    Stream.eval(exists(id).flatMap {
      // Opening it brings the instance up, so one that was running goes on and the stream can end.
      case true  => spawn(id).void
      case false => F.raiseError[Unit](FlowEngine.InstanceNotFound(id))
    }) >> hub.stream(id)

  def simulate(flow: FlowDef, inputs: Json, mocks: Map[String, Mock], realReads: Boolean): F[Trace] =
    Simulator.run(flow, inputs, mocks, realReads, registry, store.resolver, redactor, types)

  def audit(id: InstanceId): F[Option[AuditTrail]] =
    exists(id).flatMap {
      case false => F.pure(None)
      case true =>
        for {
          s <- state(id)
          entries <- hub.entries(id)
        } yield s.flow.map { f =>
          val header = AuditTrail.Header(
            s.id,
            f.id,
            f.revision,
            InstanceMachine.fingerprint(f.asJson.noSpaces),
            s.status,
            s.origin,
            redactor.inputs(f, s.inputs),
            s.startedAt,
            s.endedAt
          )
          AuditTrail(header, entries.toList)
        }
    }

  def search(query: InstanceQuery): F[Page[InstanceSummary]] = index.search(query)

}

object ActorFlowEngine {

  /** @param nodeTimeout
    *   how long a node may take. After that it fails.
    * @param secretPaths
    *   paths of the state whose values never reach the audit trail: they show as `***`. The secret fields of the configuration
    *   of the nodes are always hidden.
    */
  final case class Config(nodeTimeout: FiniteDuration = 5.minutes, secretPaths: List[StatePath] = Nil)

  /** Starts an engine.
    *
    * @param journal
    *   where the events of the instances are kept. Use [[InMemoryJournal]] to run without a database.
    * @param extraNodes
    *   kinds of node besides the ones of the core.
    * @param runnerNodes
    *   more kinds of node, for those that need to start flows: they are given the runner that does it (a stream sink that runs a
    *   flow for each element).
    * @param observer
    *   sees each entry of the audit as it happens (metrics, traces).
    * @param coreNodes
    *   whether the engine brings the nodes of the core (`core.if`...) itself. The server gives every kind of node, those of the
    *   core too, through its plugins, so it says no.
    * @param metrics
    *   where running stream segments say how they go. The nodes that run segments must be given the same one. One that is kept
    *   in memory if none is given.
    * @param index
    *   where instances are found by [[search]]. An [[InMemoryInstanceIndex]] if none is given.
    * @param store
    *   makes the store of definitions, given the kinds of node that exist, which it needs to validate the flows. An
    *   [[InMemoryFlowStore]] if none is given.
    * @param types
    *   the entities of the business model as they are each time they are asked for (they can change while the engine runs):
    *   inputs and outputs typed with an entity are made and checked by it. None if not given.
    */
  def make[F[_]: Async](
      system: ActorSystem[F],
      journal: Journal[F],
      extraNodes: List[NodeType[F]] = Nil,
      config: Config = Config(),
      store: Option[NodeCatalog => F[FlowStore[F]]] = None,
      index: Option[InstanceIndex[F]] = None,
      runnerNodes: FlowRunner[F] => List[NodeType[F]] = (_: FlowRunner[F]) => List.empty[NodeType[F]],
      metrics: Option[SegmentMetrics[F]] = None,
      types: Option[F[Types]] = None,
      coreNodes: Boolean = true,
      observer: Option[FlowObserver[F]] = None
  ): F[FlowEngine[F]] = {
    val currentTypes: F[Types] = types.getOrElse(Async[F].pure(Types.empty))
    // The instances are persistent actors that write to this same journal.
    implicit val journalBuilder: JournalBuilder[F] = () => Async[F].pure(journal)
    val persistence: Persistence[F] = IOPersistence.mkIOPersistence[F]
    // Only the descriptions of the nodes that need a runner are read here, so a runner that does nothing is enough.
    val noRunner: FlowRunner[F] = new FlowRunner[F] {
      def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: FlowRunner.Caller): F[FlowRunner.Result] =
        Async[F].pure(FlowRunner.Failed("no runner"))
    }
    val catalog = (if (coreNodes) CoreNodes.catalog else new NodeCatalog(Nil)) ++ new NodeCatalog((extraNodes ++ runnerNodes(noRunner)).map(_.descriptor))
    for {
      flows <- store.fold(InMemoryFlowStore.make[F](catalog, currentTypes).widen[FlowStore[F]])(_(catalog))
      redactor = Redactor(catalog, config.secretPaths)
      hub <- EventHub.make[F](journal, redactor, observer)
      instances <- index.fold(InMemoryInstanceIndex.make[F].widen[InstanceIndex[F]])(Async[F].pure(_))
      actors <- Ref.of[F, Map[String, ActorRef[F, Command]]](Map.empty)
      mutex <- Semaphore[F](1)
      segmentMetrics <- metrics.fold(SegmentMetrics.inMemory[F].widen[SegmentMetrics[F]])(Async[F].pure(_))
    } yield new ActorFlowEngine[F](system, flows, journal, persistence, extraNodes, runnerNodes, coreNodes, segmentMetrics, config, hub, instances, redactor, actors, mutex, currentTypes)
  }
}
