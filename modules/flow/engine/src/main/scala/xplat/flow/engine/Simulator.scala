package xplat.flow.engine

import cats.effect.Sync
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.circe.Json
import xplat.flow.engine.InstanceEvent._
import xplat.flow.engine.InstanceMachine.Launch
import xplat.flow.engine.InstanceState.Status
import xplat.flow.model._
import xplat.flow.nodes.NodeRegistry

/** Runs a flow without effects outside, to see what it would do. It uses the same rules as a real instance, one node at a
  * time, with no actors and no journal.
  *
  * What each node does:
  *   - with a mock: the mock answers.
  *   - [[SideEffects.Pure]]: it runs for real.
  *   - [[SideEffects.Write]]: it does not run. A node that calls another flow simulates that flow; any other answers with a
  *     default value of the type of each output it declares.
  *   - [[SideEffects.Read]]: it runs for real if `realReads`, otherwise it answers like a write.
  */
object Simulator {

  private val MaxDepth = 10

  def run[F[_]: Sync](
      flow: FlowDef,
      inputs: Json,
      mocks: Map[String, Mock],
      realReads: Boolean,
      registry: NodeRegistry[F],
      resolver: F[FlowResolver],
      redactor: Redactor,
      types: F[Types]
  ): F[Trace] =
    // The made-up time starts at the real one, so what a timer says it would wait for is a time that makes sense.
    Sync[F].realTime.flatMap(base => types.flatMap(t => simulate(flow, inputs, mocks, realReads, registry, resolver, redactor, 0, base.toMillis, t)))

  private def simulate[F[_]](
      flow: FlowDef,
      inputs: Json,
      mocks: Map[String, Mock],
      realReads: Boolean,
      registry: NodeRegistry[F],
      resolver: F[FlowResolver],
      redactor: Redactor,
      depth: Int,
      base: Long,
      types: Types
  )(implicit F: Sync[F]): F[Trace] = {
    val catalog = registry.catalog
    // The flow as it runs: each stream segment is one node.
    val plan = StreamSegments.condense(flow, catalog)

    /** What the members of a stream segment answer, for the segment to use instead of its sources: the elements of a mock, or a
      * failure. The segment runs for real, so what is pure in it (operators, collecting) does its work.
      */
    def segmentInput(l: Launch): NodeInput = {
      val members = StreamSegments.membersOf(l.input.params).map(_.id)
      val answers = members.flatMap { id =>
        mocks.get(id).map {
          case Mock.Output(values, _) => id -> Json.obj("elements" -> values.getOrElse("elements", Json.arr()))
          case Mock.Fail(error)       => id -> Json.obj("error" -> Json.fromString(error))
        }
      }
      l.input.copy(params = l.input.params.mapObject(_.add("simulation", Json.obj("mocks" -> Json.fromFields(answers), "realReads" -> Json.fromBoolean(realReads)))))
    }

    def answer(s: InstanceState, l: Launch): F[(String, Either[String, NodeOutput])] = {
      val node = plan.nodes.find(_.id == l.nodeId).get
      val descriptor = catalog.get(l.kind)
      def real: F[(String, Either[String, NodeOutput])] =
        registry.get(l.kind) match {
          case None    => F.pure("failed" -> Left(s"there is no node of kind '${l.kind}'"))
          case Some(t) => t.run(l.input).attempt.map(r => "ran" -> r.left.map(e => Option(e.getMessage).getOrElse(e.toString)))
        }
      def defaults: F[(String, Either[String, NodeOutput])] =
        F.pure("simulated" -> Right(NodeOutput.ok(node.outputs.map(d => d.name -> defaultOf(d.tpe, types)).toMap)))

      if (l.kind == StreamSegments.SegmentKind)
        registry.get(l.kind) match {
          case None    => F.pure("failed" -> Left(s"there is no node of kind '${l.kind}'"))
          case Some(t) => t.run(segmentInput(l)).attempt.map(r => "simulated" -> r.left.map(e => Option(e.getMessage).getOrElse(e.toString)))
        }
      else
      mocks.get(l.nodeId) match {
        case Some(Mock.Output(values, port)) => F.pure("mocked" -> Right(NodeOutput(values, port)))
        case Some(Mock.Fail(error))          => F.pure("mocked" -> Left(error))
        case None =>
          descriptor.map(_.sideEffects) match {
            case Some(SideEffects.Pure)                  => real
            case Some(SideEffects.Read) if realReads     => real
            case Some(_) =>
              descriptor.flatMap(_.calls(node.params)) match {
                case Some(call) => child(s, call).map(r => "simulated" -> r)
                // A node can say what it would have done (a message it would have sent), which the audit then shows.
                case None =>
                  registry.get(l.kind).flatMap(_.simulated(l.input)) match {
                    case Some(out) if out.failed => F.pure("simulated" -> Left(out.error.getOrElse("it failed")))
                    case Some(out)               => F.pure("simulated" -> Right(out))
                    case None                    => defaults
                  }
              }
            case None => real
          }
      }
    }

    // The flow that a node calls, simulated with what the caller gives it.
    def child(s: InstanceState, call: FlowCall): F[Either[String, NodeOutput]] =
      if (depth >= MaxDepth) F.pure(Left("flows call each other too deep"))
      else
        resolver.flatMap { r =>
          r.find(call.flowId, call.revision) match {
            case None => F.pure(Left(s"flow '${call.flowId}' does not exist"))
            case Some(callee) =>
              val filled = call.inputs.toList.map { case (name, path) => path.get(s.state).map(name -> _).toRight(s"'$path' is not in the state") }
              filled.collectFirst { case Left(e) => e } match {
                case Some(problem) => F.pure(Left(problem))
                case None =>
                  simulate(callee, Json.fromFields(filled.collect { case Right(kv) => kv }), Map.empty, realReads, registry, resolver, redactor, depth + 1, base, types).map { t =>
                    if (t.status == Status.Failed) Left(s"flow '${call.flowId}' failed")
                    else if (!call.waits) Right(NodeOutput.ok())
                    else Right(NodeOutput.ok(callee.outputs.flatMap(o => o.target.get(t.state).map(o.name -> _)).toMap))
                  }
              }
          }
        }

    def kindOf(id: String): String = plan.nodes.find(_.id == id).map(_.kind).getOrElse("")

    def step(e: InstanceEvent, after: InstanceState, outcome: String): List[Trace.Step] = e match {
      case NodeSucceeded(id, port, values, _, _, _, info, _) => List(Trace.Step(id, kindOf(id), outcome, Some(port), values, None, after.state, info))
      case NodeFailed(id, error, _, _, _, _)     => List(Trace.Step(id, kindOf(id), "failed", Some(Port.Error), Map.empty, Some(error), after.state))
      case NodeDead(id, _, _)                    => List(Trace.Step(id, kindOf(id), "skipped", None, Map.empty, None, after.state))
      case _                                     => Nil
    }

    // Time is made up, one tick for each thing that happens. A node that waits does not: it is woken at once, and the time jumps to
    // when it would have gone on, so what comes after sees the time it would have seen.
    def withWakes(state: InstanceState, events: List[InstanceEvent], tick: Long): (InstanceState, List[InstanceEvent], Long) =
      events.foldLeft((state, List.empty[InstanceEvent], tick)) { case ((st, acc, t), e) =>
        val after = InstanceMachine(st, e)
        e match {
          case w: NodeSuspended =>
            val woken = InstanceMachine.wake(after, w.nodeId, w.attempt, w.until)
            (woken.foldLeft(after)(InstanceMachine.apply), acc ++ (e :: woken), math.max(t, w.until + 1))
          case _ => (after, acc :+ e, t)
        }
      }

    def loop(s: InstanceState, events: Vector[InstanceEvent], steps: Vector[Trace.Step], marks: Map[String, AuditEntry.Simulated], clock: Long, guard: Int): F[Trace] = {
      val adv = InstanceMachine.advance(s, catalog, redactor, clock)
      val withAdv = events ++ adv.events
      // Steps of what advance decided on its own (nodes that were not reached, reused ones).
      val decided = adv.events.foldLeft((s, Vector.empty[Trace.Step])) { case ((state, acc), e) =>
        val next = InstanceMachine(state, e)
        (next, acc ++ step(e, next, "ran"))
      }._2
      val soFar = steps ++ decided

      def done(status: Status): Trace = {
        val (_, entries) = Audit.fold(InstanceMachine.empty("simulation"), withAdv.toList, redactor)
        val audit = entries.map {
          case n: AuditEntry.NodeFinished => n.copy(simulation = marks.get(n.node))
          case other                      => other
        }
        Trace(status, soFar.toList, adv.state.state, audit.toList)
      }

      if (adv.state.status != Status.Running) F.pure(done(adv.state.status))
      else if (adv.launches.isEmpty || guard <= 0) F.pure(done(Status.Failed))
      else
        adv.launches
          .foldLeft(F.pure((adv.state, withAdv, soFar, marks, clock + 1))) { (acc, l) =>
            acc.flatMap { case (state, evs, collected, ms, tick) =>
              answer(state, l).map { case (outcome, result) =>
                val mock = mocks.get(l.nodeId)
                val completed = InstanceMachine.complete(state, l.nodeId, l.attempt, result, catalog, tick, types)
                val (next, expanded, later) = withWakes(state, completed, tick)
                val newSteps = expanded.foldLeft((state, collected)) { case ((st, c), e) =>
                  val after = InstanceMachine(st, e)
                  (after, c ++ step(e, after, outcome))
                }._2
                (next, evs ++ expanded, newSteps, ms + (l.nodeId -> AuditEntry.Simulated(outcome, mock)), later + 1)
              }
            }
          }
          .flatMap { case (state, evs, collected, ms, tick) => loop(state, evs, collected, ms, tick, guard - 1) }
    }

    InstanceMachine.checkInputs(flow, inputs, types) match {
      case Left(problem) => F.raiseError[Trace](FlowEngine.InvalidInputs(problem))
      case Right(made) =>
        val start = Started(flow.id, flow.revision, flow, made, Origin.Manual(Some("simulation")), base, Some(plan).filter(_ != flow))
        loop(InstanceMachine(InstanceMachine.empty("simulation"), start), Vector(start), Vector.empty, Map.empty, base + 1L, guard = plan.nodes.size * 4 + 8)
    }
  }

  /** A value of the type, for a node whose answer is made up. A value of an entity has a value for each of its fields (an entity
    * inside itself is left empty); its rules may still refuse it, and then the node needs a mock.
    */
  def defaultOf(tpe: DataType, types: Types = Types.empty): Json = {
    def of(t: DataType, inside: Set[String]): Json = t match {
      case DataType.Bool      => Json.False
      case DataType.Num       => Json.fromInt(0)
      case DataType.Str       => Json.fromString("")
      case DataType.AnyJson   => Json.Null
      case DataType.ListOf(_) => Json.arr()
      case DataType.Obj(fs)   => Json.fromFields(fs.map { case (n, ft) => n -> of(ft, inside) })
      case DataType.Entity(e) if inside(e) => Json.obj()
      case DataType.Entity(e) => Json.fromFields(types.fields(e).getOrElse(Map.empty).map { case (n, ft) => n -> of(ft, inside + e) })
      case DataType.Ref(e)    => types.key(e).fold(Json.fromString(""))(of(_, inside))
    }
    of(tpe, Set.empty)
  }
}
