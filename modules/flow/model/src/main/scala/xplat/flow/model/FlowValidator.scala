package xplat.flow.model

/** Checks a flow before it is saved or run, without running anything. It reports every problem it finds.
  *
  * What it checks: the version; unique node ids; kinds that exist; configurations that fit their schema; edges between
  * nodes and ports that exist; no cycles; and that whatever a node reads from the state is declared, with the type it
  * expects, by the inputs of the flow or by a node before it; and that the entities its types name exist. `types` knows the
  * entities of the business model: paths are followed through their fields.
  *
  * For nodes that run another flow it also checks the call against the called flow, which the resolver finds: that it
  * exists, that every input is filled with a value of the right type, that the outputs taken are exposed with the type
  * expected, and that flows do not call each other in a circle.
  */
object FlowValidator {

  def validate(flow: FlowDef, catalog: NodeCatalog, flows: FlowResolver = FlowResolver.empty, types: Types = Types.empty): List[FlowError] = {
    val nodesById = flow.nodes.groupBy(_.id).collect { case (id, n :: _) => id -> n }

    val version = if (flow.version == FlowDef.CurrentVersion) Nil else List(FlowError.UnsupportedVersion(flow.version, FlowDef.CurrentVersion))
    val duplicates = flow.nodes.map(_.id).diff(flow.nodes.map(_.id).distinct).distinct.map(FlowError.DuplicateNode(_))
    val nodes = flow.nodes.flatMap(validateNode(_, catalog, types))
    val edges = flow.edges.flatMap(validateEdge(_, nodesById, catalog))
    val streams = validateStreams(flow, nodesById, catalog, types)
    val entities = validateEntities(flow, catalog, types)

    val known = flow.edges.filter(e => nodesById.contains(e.from) && nodesById.contains(e.to))
    val cycle = findCycle(nodesById.keys.toList, known)
    // Without a cycle every node has a well-defined set of nodes before it.
    val state = if (cycle.nonEmpty) Nil else flow.nodes.flatMap(validateReads(_, flow, nodesById, known, catalog, flows, types))
    val calls = flow.nodes.flatMap(validateCall(_, catalog, flows, types))
    val circle = findFlowCycle(flow, catalog, flows, types)

    version ++ duplicates ++ nodes ++ edges ++ streams ++ entities ++ cycle ++ state ++ calls ++ circle
  }

  private def validateNode(node: NodeDef, catalog: NodeCatalog, types: Types): List[FlowError] = {
    val dupOutputs = node.outputs.map(_.name).diff(node.outputs.map(_.name).distinct).distinct.map(FlowError.DuplicateOutput(node.id, _))
    val own = catalog.get(node.kind) match {
      // The node that stands for a stream segment is made by the engine; it is not one to write in a flow.
      case _ if node.kind == StreamSegments.SegmentKind => List(FlowError.UnknownKind(node.id, node.kind))
      case None => List(FlowError.UnknownKind(node.id, node.kind))
      case Some(d) =>
        d.config.validate(node.params, types) match {
          case Nil =>
            d.checkParams(node.params) match {
              case Nil      => Nil
              case problems => List(FlowError.InvalidParams(node.id, problems))
            }
          case problems => List(FlowError.InvalidParams(node.id, problems))
        }
    }
    own ++ dupOutputs
  }

  private def validateEdge(edge: Edge, nodes: Map[String, NodeDef], catalog: NodeCatalog): List[FlowError] = {
    val missing = List(edge.from, edge.to).distinct.filterNot(nodes.contains).map(FlowError.UnknownNode(edge, _))
    val port = for {
      from <- nodes.get(edge.from)
      d <- catalog.get(from.kind)
      available = d.portNames(from.params)
      if !available.contains(edge.port)
    } yield FlowError.UnknownPort(from.id, edge.port, available)
    val input = for {
      to <- nodes.get(edge.to)
      d <- catalog.get(to.kind)
      available = d.inputs(to.params).map(_.name)
      if !available.contains(edge.input)
    } yield FlowError.UnknownInput(to.id, edge.input, available)
    missing ++ port ++ input
  }

  /** Streams: an edge joins two ports of the same kind and, if they are streams, of compatible element types; every group of
    * nodes joined by streams has a source and a sink.
    */
  private def validateStreams(flow: FlowDef, nodes: Map[String, NodeDef], catalog: NodeCatalog, types: Types): List[FlowError] = {
    val edges = flow.edges.flatMap { e =>
      for {
        from <- nodes.get(e.from).toList
        to <- nodes.get(e.to).toList
        df <- catalog.get(from.kind).toList
        dt <- catalog.get(to.kind).toList
        error <- (df.streamOut(from.params, e.port), dt.streamIn(to.params, e.input)) match {
          case (None, None)             => Nil
          case (Some(_), None) | (None, Some(_)) => List(FlowError.StreamTokenMix(e))
          case (Some(out), Some(in))    => if (out.conformsTo(in, types)) Nil else List(FlowError.StreamTypeMismatch(e, in, out))
        }
      } yield error
    }
    val segments = StreamSegments.find(flow, catalog).flatMap { s =>
      val noSource = if (s.sources.isEmpty) List(FlowError.StreamWithoutSource(s.members)) else Nil
      val noSink = if (s.sinks.isEmpty) List(FlowError.StreamWithoutSink(s.members)) else Nil
      noSource ++ noSink
    }
    edges ++ segments
  }

  /** Every type of the flow that names an entity that `types` does not know: in its inputs and outputs, in what its nodes write
    * and in what they read.
    */
  private def validateEntities(flow: FlowDef, catalog: NodeCatalog, types: Types): List[FlowError] = {
    def unknown(t: DataType): List[String] = t.entities.toList.sorted.filter(types.fields(_).isEmpty)
    val inputs = flow.inputs.flatMap(d => unknown(d.tpe).map(FlowError.UnknownEntity(None, _, s"the input '${d.name}' of the flow")))
    val outputs = flow.outputs.flatMap(d => unknown(d.tpe).map(FlowError.UnknownEntity(None, _, s"the output '${d.name}' of the flow")))
    val nodes = flow.nodes.flatMap { n =>
      val written = n.outputs.flatMap(o => unknown(o.tpe).map(FlowError.UnknownEntity(Some(n.id), _, s"the output '${o.name}' of node '${n.id}'")))
      val read = catalog.get(n.kind).filter(_.config.validate(n.params, types).isEmpty).toList
        .flatMap(_.reads(n.params))
        .flatMap(r => unknown(r.tpe))
        .distinct
        .map(FlowError.UnknownEntity(Some(n.id), _, s"node '${n.id}'"))
      written ++ read
    }
    inputs ++ outputs ++ nodes
  }

  /** The nodes that cannot be ordered because they are in a cycle (Kahn's algorithm), if there are any. */
  private def findCycle(ids: List[String], edges: List[Edge]): List[FlowError] = {
    val incoming = scala.collection.mutable.Map(ids.map(_ -> 0): _*)
    edges.foreach(e => incoming(e.to) += 1)
    val outgoing = edges.groupBy(_.from).map { case (from, es) => from -> es.map(_.to) }
    val queue = scala.collection.mutable.Queue(ids.filter(incoming(_) == 0): _*)
    var ordered = 0
    while (queue.nonEmpty) {
      val id = queue.dequeue()
      ordered += 1
      outgoing.getOrElse(id, Nil).foreach { next =>
        incoming(next) -= 1
        if (incoming(next) == 0) queue.enqueue(next)
      }
    }
    if (ordered == ids.size) Nil else List(FlowError.Cycle(ids.filter(incoming(_) > 0).sorted))
  }

  private def validateReads(
      node: NodeDef,
      flow: FlowDef,
      nodes: Map[String, NodeDef],
      edges: List[Edge],
      catalog: NodeCatalog,
      flows: FlowResolver,
      types: Types
  ): List[FlowError] =
    catalog.get(node.kind) match {
      case Some(d) if d.config.validate(node.params, types).isEmpty =>
        val before = ancestors(node.id, edges.groupBy(_.to).map { case (to, es) => to -> es.map(_.from) })
        val declared = flow.inputs ++ before.toList.sorted.flatMap(nodes.get).flatMap(_.outputs)
        (d.reads(node.params) ++ callReads(node, d, flows)).flatMap { read =>
          // A declaration covers the path when its target is the path or a prefix of it; the rest is looked up in its type.
          val covering = declared.filter(o => read.path.startsWith(o.target))
          val candidates = covering.flatMap(o => o.tpe.at(read.path.dropPrefix(o.target).segments, types))
          // Through an entity that does not exist nothing can be said: it is reported as unknown-entity.
          if (covering.exists(_.tpe.entities.exists(types.fields(_).isEmpty))) Nil
          else if (candidates.isEmpty) List(FlowError.StateNotDeclared(node.id, read.path))
          else if (candidates.exists(_.conformsTo(read.tpe, types))) Nil
          else List(FlowError.StateTypeMismatch(node.id, read.path, read.tpe, candidates.head))
        }
      case _ => Nil
    }

  /** What the node must find in the state to fill the inputs of the flow it calls, with the types that flow expects. */
  private def callReads(node: NodeDef, d: NodeDescriptor, flows: FlowResolver): List[StateRead] =
    for {
      call <- d.calls(node.params).toList
      callee <- flows.find(call.flowId, call.revision).toList
      input <- callee.inputs
      path <- call.inputs.get(input.name).toList
    } yield StateRead(path, input.tpe)

  private def validateCall(node: NodeDef, catalog: NodeCatalog, flows: FlowResolver, types: Types): List[FlowError] =
    catalog.get(node.kind) match {
      case Some(d) if d.config.validate(node.params, types).isEmpty =>
        d.calls(node.params).toList.flatMap { call =>
          flows.find(call.flowId, call.revision) match {
            case None => List(FlowError.UnknownFlow(node.id, call.flowId, call.revision))
            case Some(callee) =>
              val inputNames = callee.inputs.map(_.name)
              val unknown = call.inputs.keys.toList.sorted.filterNot(inputNames.contains).map(FlowError.UnknownFlowInput(node.id, _))
              val missing = inputNames.filterNot(call.inputs.contains).map(FlowError.MissingFlowInput(node.id, _))
              val outputs =
                if (!call.waits && node.outputs.nonEmpty) List(FlowError.OutputsWithoutWaiting(node.id))
                else
                  node.outputs.flatMap { o =>
                    callee.outputs.find(_.name == o.name) match {
                      case None                                    => List(FlowError.UnknownFlowOutput(node.id, o.name))
                      case Some(exposed) if exposed.tpe.conformsTo(o.tpe, types) => Nil
                      case Some(exposed)                           => List(FlowError.FlowOutputMismatch(node.id, o.name, o.tpe, exposed.tpe))
                    }
                  }
              unknown ++ missing ++ outputs
          }
        }
      case _ => Nil
    }

  /** Whether `flow` ends up calling itself, through any number of other flows. */
  private def findFlowCycle(flow: FlowDef, catalog: NodeCatalog, flows: FlowResolver, types: Types): List[FlowError] = {
    def callees(f: FlowDef): List[FlowDef] =
      f.nodes
        .flatMap(n => catalog.get(n.kind).filter(_.config.validate(n.params, types).isEmpty).flatMap(_.calls(n.params)))
        .flatMap(call => flows.find(call.flowId, call.revision))
        .distinctBy(_.id)

    // Depth first from `flow`; `visited` keeps a loop that does not go through `flow` from being followed forever.
    def search(current: FlowDef, chain: List[String], visited: Set[String]): (Option[List[String]], Set[String]) =
      callees(current).foldLeft((Option.empty[List[String]], visited)) {
        case (found @ (Some(_), _), _)       => found
        case ((None, seen), callee) if callee.id == flow.id => (Some(chain :+ callee.id), seen)
        case ((None, seen), callee) if seen(callee.id)      => (None, seen)
        case ((None, seen), callee)                         => search(callee, chain :+ callee.id, seen + callee.id)
      }

    search(flow, List(flow.id), Set(flow.id))._1.map(FlowError.FlowCycle(_)).toList
  }

  private def ancestors(id: String, parents: Map[String, List[String]]): Set[String] = {
    @scala.annotation.tailrec
    def loop(pending: List[String], seen: Set[String]): Set[String] = pending match {
      case Nil => seen
      case head :: tail =>
        val next = parents.getOrElse(head, Nil).filterNot(seen)
        loop(next ++ tail, seen ++ next)
    }
    loop(List(id), Set.empty)
  }
}
