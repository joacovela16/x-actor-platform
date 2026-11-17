package xplat.flow.stream

import cats.effect.std.Queue
import cats.effect.syntax.monadCancel._
import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.circe.Json
import xplat.flow.engine.{SegmentMetrics, Simulator}
import xplat.flow.model._
import xplat.flow.nodes.NodeType

import scala.concurrent.duration._

/** Runs a whole stream segment as one node of an instance: it compiles the nodes inside it, which are in its parameters, into one
  * `fs2.Stream` and runs it.
  *
  * How the segment is run:
  *   - Each edge inside it is a small bounded queue. A node reads the queues of its inputs and writes to the queues of its output,
  *     so a slow node holds back the ones before it, and a source stops taking messages from its broker when the sink cannot
  *     keep up (backpressure). A port with several edges writes each element to all of them and waits for the slowest. A node
  *     with several output ports (see [[BranchOp]]) says for each element which one it leaves through; an element that leaves
  *     through a port with no edges is done there.
  *   - Every node of the segment is a stream that runs at the same time. If one fails, they all stop: the segment fails and the
  *     instance goes on through the `error` port of the step.
  *   - The segment ends when all its streams end, which happens when every source has ended. A source that never ends (a queue, a
  *     schedule) keeps the instance running until it is cancelled.
  *   - An element that a node cannot handle is a dead letter: it is counted, kept (the first ones) and acknowledged, and the stream
  *     goes on. Nothing is audited per element: the answer carries, for each node, counters, latency and a few samples.
  *   - Sources that were not started by an edge that fired (the other branch of an `If`) give no elements.
  *
  * It does not have a time limit (see [[NodeType.unbounded]]), and cancelling the instance cancels it, closing the sources.
  *
  * In a simulation (`simulation` in its parameters, which the simulator adds) the sources that have a mock give its elements, and
  * those that read or write outside give none or are not done, while the operators and the sinks that only collect run for real.
  */
final class SegmentNode[F[_]](
    types: Map[String, StreamNodeType[F]],
    metrics: SegmentMetrics[F],
    settings: SegmentNode.Settings = SegmentNode.Settings()
)(implicit F: Async[F])
    extends NodeType[F] {

  val descriptor: NodeDescriptor = StreamSegments.descriptor

  override def unbounded: Boolean = true

  def run(input: NodeInput): F[NodeOutput] =
    SegmentNode.Plan.parse(input.params) match {
      case Left(problem) => F.pure(NodeOutput.failed(s"invalid stream segment: $problem"))
      case Right(plan) =>
        plan.members.find(m => !types.contains(m.kind)) match {
          case Some(m) => F.pure(NodeOutput.failed(s"there is no stream node of kind '${m.kind}'"))
          case None    => execute(plan, input)
        }
    }

  private def execute(plan: SegmentNode.Plan, input: NodeInput): F[NodeOutput] = {
    val clock: F[Long] = F.realTime.map(_.toMillis)
    val simulated = plan.simulation.isDefined
    for {
      perNode <- plan.members.traverse(m => NodeMetrics.make[F](settings.sampleSize, sink = types(m.kind).isInstanceOf[StreamSink[F]]).map(m.id -> _))
      mm = perNode.toMap
      dead <- Ref.of[F, Vector[Json]](Vector.empty)
      results <- Ref.of[F, Vector[Map[String, Json]]](Vector.empty)
      links <- plan.edges.traverse(_ => (Queue.bounded[F, Option[Elem[F]]](settings.capacity), SignallingRef.of[F, Boolean](false)).mapN((q, c) => SegmentNode.Link[F](q, c)))
      edgeQueues = plan.edges.zip(links)
      nodesJson = perNode.traverse { case (id, m) => m.snapshot.map(id -> _) }.map(Json.fromFields)
      snapshot = (nodesJson, dead.get).mapN((n, d) => Json.obj("nodes" -> n, "deadLetters" -> Json.fromValues(d)))
      streams = plan.members.map(m => member(plan, input, m, mm(m.id), dead, results, edgeQueues, clock, simulated))
      reporter =
        if (simulated) Stream.empty
        else Stream.fixedDelay[F](settings.reportEvery).evalMap(_ => snapshot.flatMap(s => metrics.report(input.instanceId, input.nodeId, s)))
      // If the instance is cancelled this is cancelled too: the sources are closed and what it reported is dropped.
      outcome <- Stream.emits(streams).covary[F].parJoinUnbounded.concurrently(reporter).compile.drain.attempt.onCancel(metrics.clear(input.instanceId, input.nodeId))
      nodes <- nodesJson
      letters <- dead.get
      _ <- metrics.clear(input.instanceId, input.nodeId)
      info = Map("metrics" -> nodes, "deadLetters" -> Json.fromValues(letters))
      answer <- outcome match {
        case Right(_) => results.get.map(rs => NodeOutput(values = rs.foldLeft(Map.empty[String, Json])(_ ++ _), info = info))
        case Left(e)  => F.pure(NodeOutput(port = Port.Error, error = Some(Option(e.getMessage).getOrElse(e.toString)), info = info))
      }
    } yield answer
  }

  /** The stream of one node of the segment, which runs until its inputs end. */
  private def member(
      plan: SegmentNode.Plan,
      input: NodeInput,
      m: NodeDef,
      mine: NodeMetrics[F],
      dead: Ref[F, Vector[Json]],
      results: Ref[F, Vector[Map[String, Json]]],
      edgeQueues: List[(Edge, SegmentNode.Link[F])],
      clock: F[Long],
      simulated: Boolean
  ): Stream[F, Unit] = {
    val ctx = new StreamContext[F](m, input, simulated, mine, dead, clock, settings.deadLetterLimit)
    val incoming = edgeQueues.filter(_._1.to == m.id)
    val outgoing = edgeQueues.filter(_._1.from == m.id)

    def upstreams: Map[String, Stream[F, Elem[F]]] =
      incoming.groupBy(_._1.input).map { case (name, edges) =>
        name -> edges
          .map { case (_, l) => Stream.fromQueueNoneTerminated(l.queue).map(e => if (e.port == Port.Out) e else e.copy(port = Port.Out)).evalTap(mine.in) }
          .reduceLeft((a, b) => a.merge(b))
      }

    // An element that leaves goes to every edge of its port. It is acknowledged when all of them are done with it, or at once if
    // nothing is connected to that port.
    // An edge whose next node has ended (a `take` that has all it wanted) is closed: what would go to it is done at once.
    def fanOut(e: Elem[F]): F[Unit] =
      outgoing.filter(_._1.port == e.port).filterA(_._2.closed.get.map(!_)) >>= {
        case Nil           => e.ack
        case (_, l) :: Nil => l.offer(Some(e), e.ack)
        case many =>
          Ref.of[F, Int](many.size).flatMap { left =>
            val done = left.modify(n => (n - 1, n - 1 == 0)).flatMap(last => if (last) e.ack else F.unit)
            many.traverse_(_._2.offer(Some(e.copy(ack = done)), done))
          }
      }

    // When every edge that leaves the node is closed, nobody wants what it makes, as with the stream before a `take` in fs2. A source
    // stops then (closing its broker); any other node closes the edges that enter it, so it stops reading, and what it is still
    // working on goes to the closed edges, which acknowledge it. That way it reaches back to the sources.
    val unwanted: Stream[F, Unit] =
      if (outgoing.isEmpty) Stream.never[F]
      else outgoing.map(_._2.closed.discrete).reduceLeft((a, b) => a.zipWith(b)(_ && _)).filter(identity).take(1).void

    def publish(s: Stream[F, Elem[F]]): Stream[F, Unit] =
      // Handing an element on is not cut halfway when a source is stopped: it does not wait for long anyway, since a closed edge
      // takes it at once.
      s.evalMap(e => (clock.flatMap(t => mine.out(e, t)) >> fanOut(e)).uncancelable).drain ++ Stream.exec(outgoing.traverse_(_._2.offer(None, F.unit)))

    // Once the node has ended, the edges that enter it are closed, and what was waiting on them is done.
    val finish: F[Unit] = incoming.traverse_(_._2.close)

    val body: Stream[F, Unit] = types(m.kind) match {
      case source: StreamSource[F] =>
        // A source that has an edge into it starts if that edge fired; one that has none starts with the segment.
        val active = !plan.entries.contains(m.id) || input.firedInputs.contains(m.id)
        val elements: Stream[F, Elem[F]] =
          if (!active) Stream.empty
          else
            plan.simulation match {
              case None => source.open(ctx)
              case Some(sim) =>
                sim.mocks.get(m.id) match {
                  case Some(mock) => SegmentNode.mocked[F](mock, clock)
                  case None =>
                    val effects = source.descriptor.sideEffects
                    if (effects == SideEffects.Pure || (effects == SideEffects.Read && sim.realReads)) source.open(ctx) else Stream.empty
                }
            }
        publish(elements.interruptWhen(unwanted.as(true)))

      case operator: StreamOperator[F] => publish(operator(ctx, upstreams))

      case sink: StreamSink[F] =>
        val upstream = upstreams.getOrElse(Port.In, Stream.empty)
        val effects = sink.descriptor.sideEffects
        val pretend = simulated && effects != SideEffects.Pure && !(effects == SideEffects.Read && plan.simulation.exists(_.realReads))
        val consumed: F[Map[String, Json]] =
          if (pretend) upstream.evalMap(e => clock.flatMap(t => mine.out(e, t)) >> e.ack).compile.drain.as(m.outputs.map(o => o.name -> Simulator.defaultOf(o.tpe)).toMap)
          else sink.drain(ctx, upstream)
        // What the segment answers is by the name of the node: see [[StreamSegments.outputName]].
        Stream.eval(consumed.flatMap(r => results.update(_ :+ r.map { case (name, v) => StreamSegments.outputName(m.id, name) -> v })))

      case other => Stream.raiseError[F](new IllegalStateException(s"'${other.descriptor.kind}' is not a stream node"))
    }
    body.concurrently(unwanted.evalMap(_ => finish)).onFinalize(finish)
  }
}

object SegmentNode {

  /** An edge inside a segment: the queue of what goes along it, and whether the node it leads to has ended, after which nothing
    * goes along it any more.
    */
  final case class Link[F[_]](queue: Queue[F, Option[Elem[F]]], closed: SignallingRef[F, Boolean])(implicit F: Async[F]) {

    /** Puts `e` on the edge, waiting for room. If the edge is or becomes closed it does not wait: `ifClosed` runs instead. */
    def offer(e: Option[Elem[F]], ifClosed: F[Unit]): F[Unit] =
      closed.get.flatMap { c =>
        if (c) ifClosed
        // If it was closed just as the element went in, nobody will take it: it is acknowledged here.
        else F.race(queue.offer(e), closed.waitUntil(identity)).flatMap(_.fold(_ => closed.get.flatMap(F.whenA(_)(flush)), _ => ifClosed))
      }

    // What is waiting is acknowledged, and the end is put in its place, so whoever reads the edge stops after what it already took.
    private val flush: F[Unit] = queue.tryTakeN(None).flatMap(_.traverse_(_.traverse_(_.ack))) >> queue.tryOffer(None).void

    /** Closes the edge: what was waiting on it is acknowledged, as dropped by the node that ended, and the node after it stops reading. */
    val close: F[Unit] = closed.set(true) >> flush
  }

  /** @param capacity
    *   how many elements wait on each edge between nodes of a segment.
    * @param reportEvery
    *   how often a running segment reports its metrics, for the status of the instance.
    * @param sampleSize
    *   how many elements of the beginning each node keeps as a sample.
    * @param deadLetterLimit
    *   how many dead letters a segment keeps; the ones after are counted, not kept.
    */
  final case class Settings(capacity: Int = 16, reportEvery: FiniteDuration = 1.second, sampleSize: Int = 5, deadLetterLimit: Int = 100)

  /** What the simulator adds to the parameters of a segment: what its sources answer, by the id of the node. */
  final case class Simulation(mocks: Map[String, Json], realReads: Boolean)

  final case class Plan(
      members: List[NodeDef],
      edges: List[Edge],
      sources: List[String],
      sinks: List[String],
      entries: List[String],
      simulation: Option[Simulation]
  )

  object Plan {
    def parse(params: Json): Either[String, Plan] = {
      val c = params.hcursor
      for {
        members <- c.get[List[NodeDef]]("members").left.map(_.message)
        edges <- c.getOrElse[List[Edge]]("edges")(Nil).left.map(_.message)
        sources <- c.getOrElse[List[String]]("sources")(Nil).left.map(_.message)
        sinks <- c.getOrElse[List[String]]("sinks")(Nil).left.map(_.message)
        entries <- c.getOrElse[List[String]]("entries")(Nil).left.map(_.message)
        simulation <- c.downField("simulation").focus.traverse { s =>
          for {
            mocks <- s.hcursor.getOrElse[Map[String, Json]]("mocks")(Map.empty).left.map(_.message)
            real <- s.hcursor.getOrElse[Boolean]("realReads")(false).left.map(_.message)
          } yield Simulation(mocks, real)
        }
      } yield Plan(members, edges, sources, sinks, entries, simulation)
    }
  }

  /** What a mock says a source gives: its elements, or an error. */
  private def mocked[F[_]](mock: Json, clock: F[Long])(implicit F: Async[F]): Stream[F, Elem[F]] =
    mock.hcursor.get[String]("error") match {
      case Right(error) => Stream.raiseError[F](new RuntimeException(error))
      case Left(_) =>
        val elements = mock.hcursor.getOrElse[List[Json]]("elements")(Nil).getOrElse(Nil)
        Stream.eval(clock).flatMap(at => Stream.emits(elements.map(Elem.of[F](_, at))))
    }
}
