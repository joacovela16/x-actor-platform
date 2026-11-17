package xplat.flow.engine

import cats.effect.{Async, Ref}
import cats.implicits._
import io.circe.Json
import xplat.core.api.{Behavior, Behaviors, ReplyTo}
import xplat.core.persistence.Persistence
import xplat.core.persistence.api.{Effect, Effects}
import xplat.flow.engine.InstanceMachine.Launch
import xplat.flow.model.{FlowDef, NodeCatalog, NodeOutput, StreamSegments, Types}
import xplat.flow.nodes.NodeRegistry

import scala.concurrent.duration._

/** The actor of one instance. Its state is rebuilt from the journal, so the instance survives a restart. It decides what
  * happens next and records it as events; the nodes themselves run on their own fibers and answer with a command, so a slow
  * node never blocks the instance.
  */
object InstanceActor {

  /** The name the events of the instances are kept under in the journal. */
  val PersistenceName: String = "flow-instance"

  sealed trait Command
  final case class Start(flow: FlowDef, inputs: Json, origin: Origin, replyTo: ReplyTo[Either[String, Unit]]) extends Command
  final case class NodeDone(nodeId: String, attempt: Int, result: Either[String, NodeOutput]) extends Command
  final case class Retry(flow: Option[FlowDef], origin: Origin, replyTo: ReplyTo[Either[String, Unit]]) extends Command
  final case class Get(replyTo: ReplyTo[InstanceState]) extends Command

  /** Stops what is running: the nodes that run are cancelled and fail, the ones that were waiting do not run, and the instance
    * ends as failed. Nothing is done to an instance that has ended.
    */
  final case class Cancel(replyTo: ReplyTo[Either[String, Unit]]) extends Command

  /** Sent when the time of a timer that was waiting comes. */
  final case class Wake(nodeId: String, attempt: Int) extends Command

  /** Sent when the actor comes up: nodes that were running when the process stopped are run again. */
  case object Resume extends Command

  def behavior[F[_]](
      id: String,
      persistence: Persistence[F],
      registry: NodeRegistry[F],
      hub: EventHub[F],
      index: InstanceIndex[F],
      redactor: Redactor,
      nodeTimeout: FiniteDuration,
      types: F[Types]
  )(implicit F: Async[F]): Behavior[F, Command] =
    Behaviors.setup[F, Command] { ctx =>
      val catalog: NodeCatalog = registry.catalog

      // How to cancel each node that runs, by node and attempt, so a cancelled instance does not leave a stream running.
      val running = Ref.unsafe[F, Map[(String, Int), F[Unit]]](Map.empty)

      def launch(l: Launch): F[Unit] =
        F.start(
          NodeExecutor
            .run(registry, nodeTimeout)(l)
            .flatMap(result => running.update(_ - ((l.nodeId, l.attempt))) >> (ctx.self ! NodeDone(l.nodeId, l.attempt, result)))
            .handleError(_ => ()) // The actor is gone: nobody is waiting for the answer.
        ).flatMap(fiber => running.update(_.updated((l.nodeId, l.attempt), fiber.cancel)))

      /** The flow as the engine runs it, when it differs from what was written. */
      def planOf(flow: FlowDef): Option[FlowDef] = Some(StreamSegments.condense(flow, catalog)).filter(_ != flow)

      /** Wakes the node at `until`. If the actor is gone by then, the next one sets the timer again from the journal. */
      def timer(nodeId: String, attempt: Int, until: Long, now: Long): F[Unit] =
        F.start(F.sleep(math.max(0L, until - now).millis) >> (ctx.self ! Wake(nodeId, attempt)).handleError(_ => ())).void

      def persist(events: List[InstanceEvent]): Effects[InstanceEvent] = Effects[InstanceEvent](events.map(e => Effect.Persist(e)))

      val nothing: Effects[InstanceEvent] = Effects[InstanceEvent](Nil)

      def applyAll(state: InstanceState, events: List[InstanceEvent]): InstanceState = events.foldLeft(state)(InstanceMachine.apply)

      /** `before` is the instance as it was and `done` the events just decided on it: they take effect, the instance moves on
        * as far as it can, and everything that happened is recorded and published. `now` is the time of it all.
        */
      def proceed(before: InstanceState, done: List[InstanceEvent], now: Long, resumed: List[Launch] = Nil): F[Effects[InstanceEvent]] = {
        val next = InstanceMachine.advance(applyAll(before, done), catalog, redactor, now)
        val events = done ++ next.events
        val (after, entries) = Audit.fold(before, events, redactor)
        for {
          _ <- hub.publish(id, entries.toList)
          _ <- InstanceSummary.of(after).traverse_(index.update)
          _ <- (resumed ++ next.launches).traverse_(launch)
          // A node that starts to wait gets its timer.
          _ <- events.collect { case InstanceEvent.NodeSuspended(n, a, until, _, _) => (n, a, until) }.traverse_ { case (n, a, until) => timer(n, a, until, now) }
        } yield persist(events)
      }

      def handle(state: InstanceState, command: Command): F[Effects[InstanceEvent]] =
        F.realTime.map(_.toMillis).flatMap { now =>
          command match {
            case Start(flow, inputs, origin, replyTo) =>
              if (state.started) F.delay(replyTo ! Right(())).as(nothing)
              else
                types.flatMap { t =>
                  InstanceMachine.checkInputs(flow, inputs, t) match {
                    case Left(problem) => F.delay(replyTo ! Left(problem)).as(nothing)
                    case Right(made) =>
                      val started = InstanceEvent.Started(flow.id, flow.revision, flow, made, origin, now, planOf(flow))
                      proceed(state, List(started), now).flatTap(_ => F.delay(replyTo ! Right(())))
                  }
                }

            case NodeDone(nodeId, attempt, result) =>
              types.flatMap { t =>
                InstanceMachine.complete(state, nodeId, attempt, result, catalog, now, t) match {
                  case Nil    => F.pure(nothing)
                  case events => proceed(state, events, now)
                }
              }

            case Retry(flow, origin, replyTo) =>
              if (!state.started) F.delay(replyTo ! Left("the instance has not started")).as(nothing)
              else if (!state.finished) F.delay(replyTo ! Left("the instance is still running")).as(nothing)
              else {
                val definition = flow.orElse(state.flow).get
                val retried = InstanceEvent.Retried(definition, origin, now, planOf(definition))
                proceed(state, List(retried), now).flatTap(_ => F.delay(replyTo ! Right(())))
              }

            case Resume =>
              // The instance is known to the index again, and what was running goes on.
              // Timers that were waiting are set again: when their time has passed, they go on at once.
              InstanceSummary.of(state).traverse_(index.update) >>
                InstanceMachine.suspended(state).traverse_ { case (n, a, until) => timer(n, a, until, now) } >>
                proceed(state, Nil, now, InstanceMachine.running(state, now))

            case Wake(nodeId, attempt) =>
              InstanceMachine.wake(state, nodeId, attempt, now) match {
                case Nil    => F.pure(nothing)
                case events => proceed(state, events, now)
              }

            case Cancel(replyTo) =>
              if (!state.started) F.delay(replyTo ! Left("the instance has not started")).as(nothing)
              else if (state.finished) F.delay(replyTo ! Right(())).as(nothing)
              else {
                val ids = state.nodes.toList.sortBy(_._1)
                val failed = ids.collect { case (id, r) if r.status == InstanceState.NodeStatus.Running => InstanceEvent.NodeFailed(id, "cancelled", reused = false, at = now) }
                val dead = ids.collect { case (id, r) if r.status == InstanceState.NodeStatus.Pending => InstanceEvent.NodeDead(id, Nil, now) }
                for {
                  stops <- running.getAndSet(Map.empty)
                  _ <- stops.values.toList.traverse_(_.handleError(_ => ()))
                  effects <- proceed(state, failed ++ dead ++ List(InstanceEvent.Finished(InstanceState.Status.Failed, now)), now)
                  _ <- F.delay(replyTo ! Right(()))
                } yield effects
              }

            case Get(replyTo) => F.delay(replyTo ! state).as(nothing)
          }
        }

      persistence.build[InstanceState, Command, InstanceEvent](
        PersistenceName,
        InstanceMachine.empty(id),
        handle,
        InstanceMachine.apply,
        // The actor stays up: a node that takes long must be able to answer to it.
        Persistence.Config(shutdownOnIdle = None)
      )(Codecs.state, Codecs.event)
    }
}
