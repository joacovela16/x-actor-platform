package xplat.flow.model

import io.circe.Json
import io.circe.syntax._

/** Finds the parts of a flow that are streams, and condenses each one into a single node for the engine to run.
  *
  * A segment is a group of nodes joined by stream edges: it begins in its sources (ordinary edges lead into them and a stream
  * leaves them), goes through operators, and ends in its sinks (a stream enters them and an ordinary edge leaves). For the
  * instance the whole segment is one step. That step is represented by its first source, the `anchor`, so the id of a node
  * that exists in the flow is the one that the audit and the status show; the other nodes of the segment are inside its
  * parameters.
  */
object StreamSegments {

  /** The kind of the node that runs a whole segment. It is not in the flows that people write: the engine makes it. */
  val SegmentKind: String = "stream.segment"

  /** @param anchor
    *   the id that stands for the segment: the first of its sources.
    * @param members
    *   all its nodes, sorted.
    */
  final case class Segment(anchor: String, members: List[String], sources: List[String], sinks: List[String])

  /** The name under which an output of a node inside a segment is answered by the segment. */
  def outputName(nodeId: String, name: String): String = s"$nodeId.$name"

  /** Whether the edge carries a stream (or is meant to: either end is a stream port). */
  def isStreamEdge(edge: Edge, nodes: Map[String, NodeDef], catalog: NodeCatalog): Boolean = {
    val out = for { n <- nodes.get(edge.from); d <- catalog.get(n.kind) } yield d.streamOut(n.params, edge.port).isDefined
    val in = for { n <- nodes.get(edge.to); d <- catalog.get(n.kind) } yield d.streamIn(n.params, edge.input).isDefined
    out.contains(true) || in.contains(true)
  }

  /** The segments of the flow, ordered by anchor. A node that takes part in a stream but is joined to nothing is a segment of
    * its own, which the validator reports as lacking a source or a sink.
    */
  def find(flow: FlowDef, catalog: NodeCatalog): List[Segment] = {
    val byId = flow.nodes.groupBy(_.id).collect { case (id, n :: _) => id -> n }
    val streamNodes = flow.nodes.map(_.id).distinct.filter(id => byId.get(id).flatMap(n => catalog.get(n.kind).map(_.isStream(n.params))).contains(true))
    val parent = scala.collection.mutable.Map(streamNodes.map(id => id -> id): _*)
    def root(id: String): String = if (parent(id) == id) id else { val r = root(parent(id)); parent(id) = r; r }
    flow.edges.filter(e => parent.contains(e.from) && parent.contains(e.to) && isStreamEdge(e, byId, catalog)).foreach { e =>
      val (a, b) = (root(e.from), root(e.to))
      if (a != b) parent(a) = b
    }
    streamNodes.groupBy(root).values.toList.map { ids =>
      val members = ids.sorted
      def is(f: (NodeDescriptor, Json) => Boolean)(id: String): Boolean = byId.get(id).flatMap(n => catalog.get(n.kind).map(f(_, n.params))).contains(true)
      val sources = members.filter(is(_.isSource(_)))
      Segment(sources.headOption.getOrElse(members.head), members, sources, members.filter(is(_.isSink(_))))
    }.sortBy(_.anchor)
  }

  /** The flow as the engine runs it: each segment is one node of kind [[SegmentKind]], with the nodes inside it in its
    * parameters, and the ordinary edges that enter its sources and leave its sinks attached to it. Edges are marked with the
    * source they enter (see [[Edge.input]]). A flow without streams is returned as it is.
    */
  def condense(flow: FlowDef, catalog: NodeCatalog): FlowDef = {
    val segments = find(flow, catalog)
    if (segments.isEmpty) flow
    else {
      val byId = flow.nodes.groupBy(_.id).collect { case (id, n :: _) => id -> n }
      val anchorOf: Map[String, String] = segments.flatMap(s => s.members.map(_ -> s.anchor)).toMap
      val segmentOf = segments.map(s => s.anchor -> s).toMap
      val internal = flow.edges.filter(e => anchorOf.get(e.from).exists(a => anchorOf.get(e.to).contains(a)) && isStreamEdge(e, byId, catalog))

      def segmentNode(s: Segment): NodeDef = {
        val members = s.members.flatMap(byId.get)
        val entries = flow.edges.filter(e => s.sources.contains(e.to) && !anchorOf.get(e.from).contains(s.anchor)).map(_.to).distinct.sorted
        val reads = members.flatMap(m => catalog.get(m.kind).toList.flatMap(d => d.reads(m.params).map(_.path) ++ d.calls(m.params).toList.flatMap(_.inputs.values))).distinct
        val params = Json.obj(
          "members" -> members.asJson,
          "edges" -> internal.filter(e => anchorOf.get(e.from).contains(s.anchor)).asJson,
          "sources" -> s.sources.asJson,
          "sinks" -> s.sinks.asJson,
          "entries" -> entries.asJson,
          "reads" -> reads.asJson
        )
        val first = byId(s.anchor)
        // The outputs of the nodes inside are named by the node (`<node>.<name>`), so that two sinks that both call theirs `result` do not collide.
        NodeDef(s.anchor, SegmentKind, params, members.flatMap(m => m.outputs.map(o => o.copy(name = outputName(m.id, o.name)))), first.runPolicy, JoinPolicy.AllInputs, first.label)
      }

      val nodes = flow.nodes.flatMap { n =>
        anchorOf.get(n.id) match {
          case None                       => List(n)
          case Some(a) if a == n.id       => List(segmentNode(segmentOf(a)))
          case Some(_)                    => Nil
        }
      }
      val edges = flow.edges.diff(internal).map { e =>
        val from = anchorOf.getOrElse(e.from, e.from)
        val to = anchorOf.getOrElse(e.to, e.to)
        // Leaving a segment: only `out` and `error` exist; entering one: the edge says which source it starts.
        val port = if (anchorOf.contains(e.from)) (if (e.port == Port.Error) Port.Error else Port.Out) else e.port
        val input = if (anchorOf.contains(e.to)) e.to else e.input
        Edge(from, to, port, input)
      }.distinct
      flow.copy(nodes = nodes, edges = edges)
    }
  }

  /** The nodes of a segment node, from its parameters. */
  def membersOf(params: Json): List[NodeDef] = params.hcursor.get[List[NodeDef]]("members").getOrElse(Nil)

  def descriptor: NodeDescriptor = NodeDescriptor(
    kind = SegmentKind,
    category = "Stream",
    label = "Stream segment",
    description = "A stream of the flow, run as one step.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("members", DataType.ListOf(DataType.AnyJson)),
        ConfigSchema.Field("edges", DataType.ListOf(DataType.AnyJson), required = false),
        ConfigSchema.Field("sources", DataType.ListOf(DataType.Str), required = false),
        ConfigSchema.Field("sinks", DataType.ListOf(DataType.Str), required = false),
        ConfigSchema.Field("entries", DataType.ListOf(DataType.Str), required = false),
        ConfigSchema.Field("reads", DataType.ListOf(DataType.Str), required = false)
      )
    ),
    // What it does outside is what its members do: the engine does not run it as a simulation of itself.
    sideEffects = SideEffects.Write,
    reads = p => p.hcursor.get[List[StatePath]]("reads").getOrElse(Nil).map(StateRead(_, DataType.AnyJson))
  )

}
