package xplat.flow.engine

import io.circe.Json
import xplat.flow.engine.InstanceEvent._
import xplat.flow.engine.InstanceState.{Last, NodeRun, NodeStatus, Status, Suspension}
import xplat.flow.model._

import java.security.MessageDigest

/** The rules of an execution, as pure functions: how an event changes an instance, which nodes can go next, and what the
  * result of a node means. Nothing here runs a node or touches a journal, so the actor and the simulator share it.
  */
object InstanceMachine {

  /** A node that is ready to run, with what it needs. */
  final case class Launch(nodeId: String, kind: String, attempt: Int, input: NodeInput)

  /** What [[advance]] did: the events it produced, in order, the instance after them and the nodes to run. */
  final case class Advance(state: InstanceState, events: List[InstanceEvent], launches: List[Launch])

  def empty(id: String): InstanceState = InstanceState(id)

  // ---- events ----

  def apply(s: InstanceState, event: InstanceEvent): InstanceState = event match {
    case Started(_, _, flow, inputs, origin, at, plan) =>
      s.copy(
        flow = Some(flow),
        plan = plan,
        inputs = inputs,
        status = Status.Running,
        state = initialState(flow, inputs),
        nodes = plan.getOrElse(flow).nodes.map(_.id -> NodeRun()).toMap,
        origin = origin,
        startedAt = at,
        endedAt = None
      )

    case NodeStarted(id, attempt, hash, reads, at) =>
      update(s, id)(_.copy(status = NodeStatus.Running, attempt = attempt, hash = Some(hash), port = None, outputs = Map.empty, error = None, reads = reads, startedAt = at, waitingOn = Nil, suspended = None, info = None))

    case NodeSuspended(id, _, until, output, _) => update(s, id)(_.copy(suspended = Some(Suspension(until, output))))

    case NodeSucceeded(id, port, values, memory, reused, _, info, hash) =>
      val written = write(s, id, values)
      update(written, id) { r =>
        // A node that is reused did not start, so this is where its attempt and hash are recorded.
        val base = if (reused) r.copy(attempt = r.attempt + 1, hash = hash.orElse(r.hash), waitingOn = Nil) else r
        base.copy(status = NodeStatus.Succeeded, port = Some(port), outputs = values, error = None, memory = memory.getOrElse(r.memory), suspended = None, info = Some(info).filter(_.nonEmpty).orElse(if (reused) r.info else None))
      }

    case NodeFailed(id, error, reused, _, info, hash) =>
      update(s, id) { r =>
        val base = if (reused) r.copy(attempt = r.attempt + 1, hash = hash.orElse(r.hash), waitingOn = Nil) else r
        base.copy(status = NodeStatus.Failed, port = Some(Port.Error), outputs = Map.empty, error = Some(error), suspended = None, info = Some(info).filter(_.nonEmpty).orElse(if (reused) r.info else None))
      }

    case NodeDead(id, _, _) => update(s, id)(_.copy(status = NodeStatus.Dead, port = None, waitingOn = Nil))

    case JoinWaiting(id, pending, _) => update(s, id)(_.copy(waitingOn = pending))

    case Retried(flow, _, _, plan) =>
      val nodes = plan.getOrElse(flow).nodes.map { n =>
        val previous = s.nodes.getOrElse(n.id, NodeRun())
        val last = previous.status match {
          case NodeStatus.Succeeded => previous.hash.map(h => Last(succeeded = true, previous.port.getOrElse(Port.Out), previous.outputs, None, h))
          case NodeStatus.Failed    => previous.hash.map(h => Last(succeeded = false, Port.Error, Map.empty, previous.error, h))
          case _                    => previous.last
        }
        n.id -> NodeRun(memory = previous.memory, last = last)
      }
      s.copy(flow = Some(flow), plan = plan, status = Status.Running, state = initialState(flow, s.inputs), nodes = nodes.toMap, endedAt = None)

    case Finished(status, at) => s.copy(status = status, endedAt = Some(at))
  }

  private def update(s: InstanceState, id: String)(f: NodeRun => NodeRun): InstanceState =
    s.copy(nodes = s.nodes.updated(id, f(s.nodes.getOrElse(id, NodeRun()))))

  /** The state at the start: each input of the flow at its target. */
  private def initialState(flow: FlowDef, inputs: Json): Json =
    flow.inputs.foldLeft(Json.obj()) { (state, decl) =>
      inputs.hcursor.downField(decl.name).focus.fold(state)(decl.target.set(state, _))
    }

  /** Each value at the target that its node declared for it. */
  private def write(s: InstanceState, id: String, values: Map[String, Json]): InstanceState = {
    val decls = s.graph.toList.flatMap(_.nodes.find(_.id == id)).flatMap(_.outputs)
    s.copy(state = decls.foldLeft(s.state)((state, decl) => values.get(decl.name).fold(state)(decl.target.set(state, _))))
  }

  // ---- starting ----

  /** The inputs the instance starts with, if it can start with these: every input of the flow is there, with its type, and no
    * more. An input whose type is an entity (or holds one) is made by it: its defaults and computed fields are filled in.
    */
  def checkInputs(flow: FlowDef, inputs: Json, types: Types = Types.empty): Either[String, Json] = {
    val obj = if (inputs.isNull) Json.obj() else inputs
    obj.asObject match {
      case None => Left("the inputs must be an object")
      case Some(o) =>
        val declared = flow.inputs.map(_.name).toSet
        val unknown = o.keys.filterNot(declared).toList.sorted.map(n => s"unknown input '$n'")
        val checked = flow.inputs.map { d =>
          o(d.name) match {
            case None => Left(s"missing input '${d.name}'")
            case Some(v) =>
              d.tpe.conform(v, types).map(d.name -> _).left.map(ps => s"input '${d.name}' ${explain(d.tpe, ps)}")
          }
        }
        val problems = checked.collect { case Left(p) => p }
        if (problems.isEmpty && unknown.isEmpty) Right(Json.fromJsonObject(checked.collect { case Right(f) => f }.foldLeft(o) { case (acc, (n, v)) => acc.add(n, v) }))
        else Left((problems ++ unknown).mkString("; "))
    }
  }

  /** What is wrong with a value of `tpe`: for a plain type, that it must be of it; for a value with entities, each problem with its
    * path.
    */
  private def explain(tpe: DataType, problems: List[DataType.Problem]): String =
    if (tpe.entities.isEmpty) s"must be ${tpe.render}" else s"is not a valid ${tpe.render}: ${problems.map(_.render).mkString(", ")}"

  // ---- what comes next ----

  private sealed trait EdgeState
  private case object Fired extends EdgeState
  private case object Silent extends EdgeState
  private case object Open extends EdgeState

  private sealed trait Readiness
  private case object Ready extends Readiness
  private case object Never extends Readiness
  private case object Waiting extends Readiness

  private def edgeState(s: InstanceState, edge: Edge): EdgeState = {
    val source = s.nodes.getOrElse(edge.from, NodeRun())
    source.status match {
      case NodeStatus.Succeeded | NodeStatus.Failed => if (source.port.contains(edge.port)) Fired else Silent
      case NodeStatus.Dead                          => Silent
      case _                                        => Open
    }
  }

  /** An edge that will never fire counts as arrived, so a join does not wait for a branch that was not taken. */
  private def readiness(s: InstanceState, flow: FlowDef, node: NodeDef): Readiness = {
    val states = flow.edges.filter(_.to == node.id).map(edgeState(s, _))
    if (states.isEmpty) Ready
    else
      node.joinPolicy match {
        case JoinPolicy.AllInputs =>
          if (states.contains(Open)) Waiting else if (states.contains(Fired)) Ready else Never
        case JoinPolicy.AnyInput =>
          if (states.contains(Fired)) Ready else if (states.contains(Open)) Waiting else Never
      }
  }

  private val Hex = "0123456789abcdef"

  /** A short fingerprint of a text. */
  private[engine] def fingerprint(text: String): String = {
    val bytes = MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))
    bytes.take(12).map(b => s"${Hex((b >> 4) & 0xf)}${Hex(b & 0xf)}").mkString
  }

  /** Identifies what a node ran with: its configuration and the values it declares that it reads, including the ones that
    * fill the inputs of a flow it calls. If it is the same, a run can be reused.
    */
  def hashOf(s: InstanceState, node: NodeDef, catalog: NodeCatalog): String = {
    val read = paths(node, catalog).map(p => p.render + "=" + p.get(s.state).fold("-")(_.noSpaces))
    fingerprint(node.kind + "|" + node.params.noSpaces + "|" + read.mkString(";"))
  }

  /** Moves the instance forward as far as it can without running a node: starts the nodes that are ready, reuses those that
    * their run policy says to, marks as dead those that will not be reached, and finishes the instance when nothing is left.
    * The nodes that have to run are returned as launches.
    */
  def advance(initial: InstanceState, catalog: NodeCatalog, redactor: Redactor, now: Long): Advance = {
    var s = initial
    val events = List.newBuilder[InstanceEvent]
    val launches = List.newBuilder[Launch]

    def emit(e: InstanceEvent): Unit = { events += e; s = apply(s, e) }

    s.graph match {
      case Some(flow) if s.status == Status.Running =>
        var progress = true
        while (progress) {
          progress = false
          flow.nodes.foreach { node =>
            val run = s.nodes.getOrElse(node.id, NodeRun())
            if (run.status == NodeStatus.Pending) {
              readiness(s, flow, node) match {
                case Waiting =>
                  // Some edges are in and others are not: say which are missing, once.
                  val edges = flow.edges.filter(_.to == node.id)
                  val states = edges.map(e => e -> edgeState(s, e))
                  val pending = states.collect { case (e, Open) => s"${e.from}:${e.port}" }
                  if (states.exists(_._2 != Open) && pending != run.waitingOn) emit(JoinWaiting(node.id, pending, now))
                case Never =>
                  emit(NodeDead(node.id, causes(s, flow, node), now))
                  progress = true
                case Ready =>
                  val hash = hashOf(s, node, catalog)
                  run.last.filter(_.hash == hash) match {
                    case Some(l) if l.succeeded && node.runPolicy.whenSucceeded == RunPolicy.Skip =>
                      emit(NodeSucceeded(node.id, l.port, l.outputs, None, reused = true, at = now, hash = Some(hash)))
                    case Some(l) if !l.succeeded && node.runPolicy.whenFailed == RunPolicy.Skip =>
                      emit(NodeFailed(node.id, l.error.getOrElse("failed"), reused = true, at = now, hash = Some(hash)))
                    case _ =>
                      emit(NodeStarted(node.id, run.attempt + 1, hash, readsOf(s, node, catalog, redactor), now))
                      launches += launch(s, node, run.attempt + 1, now)
                  }
                  progress = true
              }
            }
          }
        }
        val pending = s.nodes.values.exists(r => r.status == NodeStatus.Pending || r.status == NodeStatus.Running)
        if (!pending) emit(Finished(outcome(s, flow), now))
      case _ => ()
    }
    Advance(s, events.result(), launches.result())
  }

  /** What a node reads, with the secret values hidden. */
  private def readsOf(s: InstanceState, node: NodeDef, catalog: NodeCatalog, redactor: Redactor): List[AuditEntry.Read] =
    paths(node, catalog).distinct.map(p => AuditEntry.Read(p.render, redactor.value(p, p.get(s.state))))

  private def paths(node: NodeDef, catalog: NodeCatalog): List[StatePath] =
    catalog.get(node.kind).toList.flatMap(d => d.reads(node.params).map(_.path) ++ d.calls(node.params).toList.flatMap(_.inputs.values))

  /** Why each edge that leads to the node did not fire, for the audit. */
  private def causes(s: InstanceState, flow: FlowDef, node: NodeDef): List[AuditEntry.Cause] =
    flow.edges.filter(_.to == node.id).map { e =>
      val source = s.nodes.getOrElse(e.from, NodeRun())
      source.status match {
        case NodeStatus.Succeeded | NodeStatus.Failed => AuditEntry.Cause(e.from, e.port, "other-port", source.port, source.reads)
        case _                                        => AuditEntry.Cause(e.from, e.port, "source-not-run", None, Nil)
      }
    }

  /** It failed if a node failed and nothing was there to take the failure through its error port. */
  private def outcome(s: InstanceState, flow: FlowDef): Status = {
    val unhandled = flow.nodes.exists { n =>
      s.nodes.get(n.id).exists(_.status == NodeStatus.Failed) && !flow.edges.exists(e => e.from == n.id && e.port == Port.Error)
    }
    if (unhandled) Status.Failed else Status.Succeeded
  }

  /** The nodes that were running, as launches again: what an instance does when it comes back from a crash. A timer that was
    * waiting does not run again: see [[suspended]].
    */
  def running(s: InstanceState, now: Long): List[Launch] =
    s.graph.toList.flatMap { flow =>
      flow.nodes.flatMap { node =>
        s.nodes.get(node.id).filter(r => r.status == NodeStatus.Running && r.suspended.isEmpty).map(r => launch(s, node, r.attempt, now))
      }
    }

  /** The timers that are waiting, as (node, attempt, time it goes on): what an instance sets again when it comes back from a
    * crash.
    */
  def suspended(s: InstanceState): List[(String, Int, Long)] =
    s.nodes.toList.sortBy(_._1).collect { case (id, r) if r.status == NodeStatus.Running && r.suspended.isDefined => (id, r.attempt, r.suspended.get.until) }

  /** The events for a timer whose time has come: the node finishes with what it answered when it started to wait. Nothing if it
    * is not waiting in that attempt (it came late, or twice).
    */
  def wake(s: InstanceState, nodeId: String, attempt: Int, now: Long): List[InstanceEvent] =
    s.nodes.get(nodeId) match {
      case Some(r) if r.status == NodeStatus.Running && r.attempt == attempt && r.suspended.isDefined =>
        val o = r.suspended.get.output
        List(NodeSucceeded(nodeId, o.port, o.values, o.memory, reused = false, at = now, info = o.info))
      case _ => Nil
    }

  private def launch(s: InstanceState, node: NodeDef, attempt: Int, now: Long): Launch = {
    val entry = !s.graph.exists(_.edges.exists(_.to == node.id))
    // The inputs through which an edge fired: a stream segment starts only the sources whose edge did.
    val fired = s.graph.toList.flatMap(_.edges.filter(_.to == node.id)).filter(e => edgeState(s, e) == Fired).map(_.input).distinct
    Launch(node.id, node.kind, attempt, NodeInput(node.params, s.state, s.nodes.get(node.id).map(_.memory).getOrElse(Json.obj()), s.id, node.id, now, entry, fired, node.outputs))
  }

  // ---- what a node answered ----

  /** The events for the answer of a node. An answer for a node that is not running that attempt (it came late, after a
    * retry) is ignored. Whatever does not fit what the node declared makes it fail, instead of writing it in the state.
    */
  def complete(
      s: InstanceState,
      nodeId: String,
      attempt: Int,
      result: Either[String, NodeOutput],
      catalog: NodeCatalog,
      now: Long,
      types: Types = Types.empty
  ): List[InstanceEvent] = {
    val run = s.nodes.get(nodeId)
    val node = s.graph.flatMap(_.nodes.find(_.id == nodeId))
    (run, node) match {
      case (Some(r), Some(n)) if r.status == NodeStatus.Running && r.attempt == attempt =>
        check(n, catalog, result, types) match {
          case Left(error)   => List(NodeFailed(nodeId, error, reused = false, at = now, info = result.toOption.fold(Map.empty[String, Json])(_.info)))
          // A node that waits stays running until its time comes; a time that is not in the future is no wait.
          case Right(output) =>
            output.suspendUntil match {
              case Some(until) if until > now => List(NodeSuspended(nodeId, attempt, until, output.copy(suspendUntil = None), now))
              case _                          => List(NodeSucceeded(nodeId, output.port, output.values, output.memory, reused = false, at = now, info = output.info))
            }
        }
      case _ => Nil
    }
  }

  /** The answer of a node as it is written: it leaves through a port it has and answers what it declared (what that port writes),
    * each value with its type; values of entity types are made by their entity. Anything else makes it fail.
    */
  private def check(node: NodeDef, catalog: NodeCatalog, result: Either[String, NodeOutput], types: Types): Either[String, NodeOutput] =
    result.flatMap { out =>
      out.error match {
        case Some(error) => Left(error)
        case None if out.port == Port.Error => Left("it failed through its error port without saying why")
        case None =>
          val described = catalog.get(node.kind).map(_.ports(node.params))
          val ports = catalog.get(node.kind).map(_.portNames(node.params)).getOrElse(List(Port.Out, Port.Error))
          val writes = described.flatMap(_.find(_.name == out.port)).flatMap(_.writes)
          val declared = node.outputs.map(o => o.name -> o).toMap
          val unknownPort = if (ports.contains(out.port)) Nil else List(s"it answered through the port '${out.port}', which it does not have")
          val unknown = out.values.keys.toList.sorted.filterNot(declared.contains).map(n => s"it answered '$n', which it did not declare")
          val expected = if (out.port == Port.Error) Nil else node.outputs.filter(d => writes.forall(_.contains(d.name)))
          val checked = expected.map { d =>
            out.values.get(d.name) match {
              case None => Left(s"it did not answer '${d.name}'")
              case Some(v) =>
                d.tpe.conform(v, types).map(d.name -> _).left.map(ps => s"'${d.name}' ${explain(d.tpe, ps)}")
            }
          }
          val all = unknownPort ++ unknown ++ checked.collect { case Left(p) => p }
          if (all.isEmpty) Right(out.copy(values = out.values ++ checked.collect { case Right(v) => v })) else Left(all.mkString("; "))
      }
    }
}
