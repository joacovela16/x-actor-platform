package xplat.flow.model

/** Finds the flows that other flows call. */
trait FlowResolver {

  /** The flow with that id and revision, or its latest revision when `revision` is empty. */
  def find(flowId: String, revision: Option[Int]): Option[FlowDef]
}

object FlowResolver {

  /** Knows no flow, so every call to another flow is reported as unknown. */
  val empty: FlowResolver = (_, _) => None

  def of(flows: Iterable[FlowDef]): FlowResolver = {
    val byId = flows.groupBy(_.id)
    (flowId, revision) =>
      byId.get(flowId).flatMap { revisions =>
        revision match {
          case Some(r) => revisions.find(_.revision == r)
          case None    => Some(revisions.maxBy(_.revision))
        }
      }
  }
}
