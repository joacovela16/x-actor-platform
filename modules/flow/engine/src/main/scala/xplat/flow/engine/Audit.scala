package xplat.flow.engine

import io.circe.syntax._
import xplat.flow.engine.AuditEntry.{Decision, EdgeDead, EdgeFired, EdgeRef, InstanceFinished, InstanceStarted, NodeFinished, NodeSkipped, NodeWaiting, RetryRequested, StateChanged, Written}
import xplat.flow.engine.InstanceEvent._
import xplat.flow.engine.InstanceState.Status
import xplat.flow.model.{FlowDef, Port}

/** Makes the audit trail out of the events of an instance. It is a function of the events, so the trail that is followed
  * live and the one read later from the journal are the same.
  */
object Audit {

  /** The entries that `event` makes, given the instance as it was before it. They are not numbered yet. */
  def project(before: InstanceState, event: InstanceEvent, redactor: Redactor): List[AuditEntry] = {
    val flow = before.graph
    def run(id: String) = before.nodes.getOrElse(id, InstanceState.NodeRun())
    def kindOf(id: String): String = flow.flatMap(_.nodes.find(_.id == id)).map(_.kind).getOrElse("")

    /** What the node said about its run apart from the link to a child instance (`info` is for the audit only). */
    def extra(info: Map[String, io.circe.Json]): Map[String, io.circe.Json] = info - xplat.flow.nodes.SubflowNode.ChildInstance

    /** The values written, by the declaration of the node, with what they replace. */
    def written(id: String, values: Map[String, io.circe.Json]): List[Written] =
      flow.toList.flatMap(_.nodes.find(_.id == id)).flatMap(_.outputs).flatMap { d =>
        values.get(d.name).map { v =>
          Written(d.name, d.target.render, redactor.value(d.target, d.target.get(before.state)), redactor.value(d.target, v))
        }
      }

    /** What the node changed in the state, if it changed anything. */
    def changed(id: String, attempt: Int, changes: List[Written], at: Long): List[AuditEntry] =
      if (changes.isEmpty) Nil else List(StateChanged(0, at, id, attempt, changes))

    /** What the edges that leave the node do: the ones of the port taken fire and the others die. */
    def edges(id: String, taken: Option[String], at: Long): List[AuditEntry] =
      flow.toList.flatMap(_.edges.filter(_.from == id)).map { e =>
        val ref = EdgeRef(e.from, e.port, e.to)
        taken match {
          case Some(p) if p == e.port => EdgeFired(0, at, ref)
          case Some(p)                => EdgeDead(0, at, ref, s"the node left through '$p'")
          case None                   => EdgeDead(0, at, ref, "the node did not run")
        }
      }

    event match {
      case Started(flowId, revision, f, inputs, origin, at, _) =>
        List(InstanceStarted(0, at, before.id, flowId, revision, InstanceMachine.fingerprint(f.asJson.noSpaces), origin, redactor.inputs(f, inputs)))

      case NodeStarted(id, attempt, hash, reads, at) =>
        val params = flow.flatMap(_.nodes.find(_.id == id)).fold(io.circe.Json.obj())(redactor.params)
        List(AuditEntry.NodeStarted(0, at, id, kindOf(id), attempt, hash, params, reads, run(id).memory))

      case NodeSucceeded(id, port, values, memory, reused, at, info, _) =>
        if (reused) {
          val r = run(id)
          val changes = written(id, values)
          val decision = Decision.RunPolicy("whenSucceeded=skip", port, changes, None)
          NodeSkipped(0, at, id, kindOf(id), r.attempt + 1, decision) :: changed(id, r.attempt + 1, changes, at) ++ edges(id, Some(port), at)
        } else {
          val r = run(id)
          val changes = written(id, values)
          NodeFinished(0, at, id, kindOf(id), r.attempt, r.startedAt, at - r.startedAt, "succeeded", port, None, changes, memory, info.get(xplat.flow.nodes.SubflowNode.ChildInstance).flatMap(_.asString), None, extra(info)) ::
            changed(id, r.attempt, changes, at) ++ edges(id, Some(port), at)
        }

      case NodeFailed(id, error, reused, at, info, _) =>
        val r = run(id)
        if (reused) NodeSkipped(0, at, id, kindOf(id), r.attempt + 1, Decision.RunPolicy("whenFailed=skip", Port.Error, Nil, Some(error))) :: edges(id, Some(Port.Error), at)
        else
          NodeFinished(0, at, id, kindOf(id), r.attempt, r.startedAt, at - r.startedAt, "failed", Port.Error, Some(error), Nil, None, info.get(xplat.flow.nodes.SubflowNode.ChildInstance).flatMap(_.asString), None, extra(info)) ::
            edges(id, Some(Port.Error), at)

      case NodeDead(id, causes, at) =>
        NodeSkipped(0, at, id, kindOf(id), run(id).attempt + 1, Decision.DeadBranch(causes)) :: edges(id, None, at)

      case NodeSuspended(id, _, until, _, at) =>
        List(NodeWaiting(0, at, id, Decision.Scheduled(until)))

      case JoinWaiting(id, pending, at) =>
        List(NodeWaiting(0, at, id, Decision.WaitingJoin(pending.flatMap(parseEdge(flow, id, _)))))

      case Retried(f, origin, at, _) => List(RetryRequested(0, at, origin, f.revision))

      case InstanceEvent.Finished(status, at) => List(InstanceFinished(0, at, status.name, at - before.startedAt))
    }
  }

  private def parseEdge(flow: Option[FlowDef], to: String, key: String): Option[EdgeRef] =
    key.split(":", 2) match {
      case Array(from, port) => Some(EdgeRef(from, port, to))
      case _                 => None
    }

  /** The entries that `events` make, in order and numbered from `first`, and the instance after them. */
  def fold(start: InstanceState, events: List[InstanceEvent], redactor: Redactor, first: Long = 0L): (InstanceState, Vector[AuditEntry]) =
    events.foldLeft((start, Vector.empty[AuditEntry])) { case ((state, acc), event) =>
      val entries = project(state, event, redactor).zipWithIndex.map { case (e, i) => e.withSeq(first + acc.size + i) }
      (InstanceMachine(state, event), acc ++ entries)
    }

  /** The whole trail of an instance from its events. */
  def fromEvents(id: String, events: Iterable[InstanceEvent], redactor: Redactor): Vector[AuditEntry] =
    fold(InstanceMachine.empty(id), events.toList, redactor)._2

  def statusName(s: Status): String = s.name
}
