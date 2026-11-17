package xplat.flow.stream

import cats.effect.{Async, Resource}
import xplat.flow.engine.SegmentMetrics
import xplat.flow.model.{NodeCatalog, StreamSegments}
import xplat.flow.nodes.{Connections, FlowRunner, NodeType, StateStore, Traces}
import xplat.flow.queue.{ConsumerFactory, PublisherFactory, PublisherPool}

/** The nodes that process streams, to give to the engine.
  *
  * {{{
  * val metrics = SegmentMetrics.inMemory[F]
  * StreamNodes.resource[F](connections, metrics).use { nodes =>
  *   ActorFlowEngine.make[F](system, journal, runnerNodes = nodes, metrics = Some(metrics))
  * }
  * }}}
  *
  * They are given to the engine as `runnerNodes` because the sink that runs a flow for each element needs the engine's runner. The
  * same `metrics` must be given to the engine, which shows them in the status of the instance.
  */
object StreamNodes {

  /** The descriptions of the nodes, for validation and the editor, without anything to run. */
  val catalog: NodeCatalog = new NodeCatalog(
    List(
      ListSource.descriptor,
      RangeSource.descriptor,
      CronSource.descriptor,
      QueueSource.descriptor,
      MapOp.descriptor,
      FilterOp.descriptor,
      BranchOp.descriptor,
      EvalMapOp.descriptor,
      TakeOp.descriptor,
      DropOp.descriptor,
      TakeWhileOp.descriptor,
      DropWhileOp.descriptor,
      InterruptAfterOp.descriptor,
      ScanOp.descriptor,
      ChangesOp.descriptor,
      ZipWithIndexOp.descriptor,
      ZipWithPreviousOp.descriptor,
      ZipWithNextOp.descriptor,
      GroupAdjacentByOp.descriptor,
      FlatMapOp.descriptor,
      WindowOp.descriptor,
      AggregateOp.descriptor,
      DedupeOp.descriptor,
      ThrottleOp.descriptor,
      MergeOp.descriptor,
      ZipOp.descriptor,
      JoinOp.descriptor,
      CollectSink.descriptor,
      ReduceSink.descriptor,
      FlowSink.descriptor,
      PublishSink.descriptor,
      HttpSink.descriptor,
      StreamSegments.descriptor
    )
  )

  /** Every kind of node of a stream. */
  def types[F[_]](
      connections: Connections[F],
      consumers: ConsumerFactory[F],
      pool: PublisherPool[F],
      runner: FlowRunner[F],
      extra: List[StreamNodeType[F]] = Nil,
      traces: Option[Traces[F]] = None
  )(implicit F: Async[F]): List[StreamNodeType[F]] =
    extra ++ List[StreamNodeType[F]](
      new ListSource[F],
      new RangeSource[F],
      new CronSource[F],
      new QueueSource[F](connections, consumers),
      new MapOp[F],
      new FilterOp[F],
      new BranchOp[F],
      new EvalMapOp[F](runner),
      new TakeOp[F],
      new DropOp[F],
      new TakeWhileOp[F],
      new DropWhileOp[F],
      new InterruptAfterOp[F],
      new ScanOp[F],
      new ChangesOp[F],
      new ZipWithIndexOp[F],
      new ZipWithPreviousOp[F],
      new ZipWithNextOp[F],
      new GroupAdjacentByOp[F],
      new FlatMapOp[F],
      new WindowOp[F],
      new AggregateOp[F],
      new DedupeOp[F],
      new ThrottleOp[F],
      new MergeOp[F],
      new ZipOp[F],
      new JoinOp[F],
      new CollectSink[F],
      new ReduceSink[F],
      new FlowSink[F](runner),
      new PublishSink[F](connections, pool, traces),
      new HttpSink[F](traces)
    )

  /** The nodes for the engine: the one that runs a segment, and the description of each node that can be in one. */
  def nodes[F[_]](
      connections: Connections[F],
      consumers: ConsumerFactory[F],
      pool: PublisherPool[F],
      metrics: SegmentMetrics[F],
      runner: FlowRunner[F],
      settings: SegmentNode.Settings = SegmentNode.Settings(),
      extra: List[StreamNodeType[F]] = Nil,
      traces: Option[Traces[F]] = None,
      state: Option[StateStore[F]] = None
  )(implicit F: Async[F]): List[NodeType[F]] = {
    val all = types[F](connections, consumers, pool, runner, extra, traces)
    new SegmentNode[F](all.map(t => t.descriptor.kind -> t).toMap, metrics, settings, state) :: all.map(new StreamMember[F](_))
  }

  /** The nodes for the engine's `runnerNodes`, with the broker consumers and publishers of the connectors. The publishers it opens
    * are closed when the resource is released. `extra` are kinds of stream node from other modules (`stream.script`).
    */
  def resource[F[_]](
      connections: Connections[F],
      metrics: SegmentMetrics[F],
      settings: SegmentNode.Settings = SegmentNode.Settings(),
      extra: List[StreamNodeType[F]] = Nil
  )(implicit F: Async[F]): Resource[F, FlowRunner[F] => List[NodeType[F]]] =
    PublisherPool.make[F](PublisherFactory.connectors[F]).map { pool => (runner: FlowRunner[F]) =>
      nodes[F](connections, ConsumerFactory.connectors[F], pool, metrics, runner, settings, extra)
    }
}
