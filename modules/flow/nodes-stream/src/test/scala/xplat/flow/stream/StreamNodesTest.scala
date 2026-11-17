package xplat.flow.stream

import cats.effect.IO
import io.circe.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.engine.SegmentMetrics
import xplat.flow.http.ApiJson
import xplat.flow.model._
import xplat.flow.nodes.{Connections, FlowRunner, NodeRegistry}
import xplat.flow.queue.{ConsumerFactory, PublisherPool}

class StreamNodesTest extends AnyFreeSpec with Matchers {

  private val noConnections: Connections[IO] = (_: String) => IO.pure(None)
  private val noPool: PublisherPool[IO] = new PublisherPool[IO] {
    def use[A](c: xplat.flow.nodes.Connections.Resolved, d: String, t: String)(f: xplat.core.connectors.Publisher[IO, Array[Byte]] => IO[A]): IO[A] = IO.raiseError(new IllegalStateException("no pool"))
  }
  private val noConsumers: ConsumerFactory[IO] = (_, _, _) => throw new IllegalStateException("no consumers")
  private val noRunner: FlowRunner[IO] = (_, _, _, _, _) => IO.pure(FlowRunner.Finished(Map.empty))

  private val types = StreamNodes.types[IO](noConnections, noConsumers, noPool, noRunner)

  "the catalog describes every node that can run in a stream, and the node of the segment" in {
    StreamNodes.catalog.all.map(_.kind).toSet shouldBe types.map(_.descriptor.kind).toSet + StreamSegments.SegmentKind
    types.map(_.descriptor.kind).distinct.size shouldBe types.size
  }

  "every kind says what it is in a stream, and its category groups it in the palette" in {
    val roles = types.map(t => t.descriptor.kind -> ApiJson.role(t.descriptor, Json.obj())).toMap
    roles.filter(_._2 == "source").keySet shouldBe Set("stream.source.list", "stream.source.range", "stream.source.cron", "stream.source.queue")
    roles.filter(_._2 == "sink").keySet shouldBe Set("stream.sink.collect", "stream.sink.reduce", "stream.sink.flow", "stream.sink.publish", "stream.sink.http")
    roles.filter(_._2 == "operator").keySet shouldBe Set("stream.map", "stream.filter", "stream.branch", "stream.evalMap", "stream.take", "stream.drop", "stream.takeWhile", "stream.dropWhile", "stream.interruptAfter", "stream.scan", "stream.changes", "stream.zipWithIndex", "stream.zipWithPrevious", "stream.zipWithNext", "stream.groupAdjacentBy", "stream.flatMap", "stream.window", "stream.aggregate", "stream.dedupe", "stream.throttle", "stream.merge", "stream.zip", "stream.join")
    types.foreach { t =>
      (t, t.descriptor.category) match {
        case (_: StreamSource[IO], c)   => c shouldBe StreamPorts.SourceCategory
        case (_: StreamOperator[IO], c) => c shouldBe StreamPorts.OperatorCategory
        case (_: StreamSink[IO], c)     => c shouldBe StreamPorts.SinkCategory
        case other                      => fail(s"not a stream node: $other")
      }
    }
    succeed
  }

  "what the class is and what the description says agree" in {
    types.foreach {
      case s: StreamSource[IO]   => withClue(s.descriptor.kind) { s.descriptor.isSource(Json.obj()) shouldBe true }
      case o: StreamOperator[IO] => withClue(o.descriptor.kind) { (o.descriptor.inputs(Json.obj()).exists(_.stream.isDefined) && o.descriptor.ports(Json.obj()).exists(_.stream.isDefined)) shouldBe true }
      case k: StreamSink[IO]     => withClue(k.descriptor.kind) { k.descriptor.isSink(Json.obj()) shouldBe true }
      case _                     => fail("not a stream node")
    }
    succeed
  }

  "the palette entry has the inputs, the ports with their stream type and the role" in {
    val entry = ApiJson.catalogEntry(JoinOp.descriptor)
    entry.hcursor.get[String]("role") shouldBe Right("operator")
    entry.hcursor.downField("inputs").as[List[Port]].toOption.get.map(_.name) shouldBe List("left", "right")
    entry.hcursor.downField("inputs").downArray.downField("stream").focus should not be empty
    entry.hcursor.downField("ports").as[List[Port]].toOption.get.map(_.name) shouldBe List("out", "error")
    entry.hcursor.downField("ports").downArray.downField("stream").focus should not be empty
    ApiJson.catalogEntry(CollectSink.descriptor).hcursor.downField("ports").as[List[Port]].toOption.get.map(_.stream) shouldBe List(None, None)
    ApiJson.catalogEntry(IfNodeDescriptor).hcursor.get[String]("role") shouldBe Right("node")
  }

  private val IfNodeDescriptor = xplat.flow.nodes.IfNode.descriptor

  "the nodes for the engine have the segment and one description for each kind, with no kind twice" in {
    SegmentMetrics.inMemory[IO].map { metrics =>
      val nodes = StreamNodes.nodes[IO](noConnections, noConsumers, noPool, metrics, noRunner)
      nodes.map(_.descriptor.kind).toSet shouldBe StreamNodes.catalog.all.map(_.kind).toSet
      new NodeRegistry[IO](nodes).get(StreamSegments.SegmentKind).get.unbounded shouldBe true
      nodes.count(_.unbounded) shouldBe 1
    }.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  "a stream node cannot be run as if it were a node of the instance" in {
    val member = new StreamMember[IO](types.head)
    member.run(NodeInput(Json.obj(), Json.obj())).unsafeRunSync()(cats.effect.unsafe.implicits.global).failed shouldBe true
  }

  "a configuration that is wrong is told when the flow is validated, for every kind that has required fields" in {
    // An empty configuration is wrong for the nodes that need something; the validator says so before anything runs.
    val needing = List(ListSource.descriptor, QueueSource.descriptor, FilterOp.descriptor, FlatMapOp.descriptor, JoinOp.descriptor, FlowSink.descriptor, PublishSink.descriptor, HttpSink.descriptor, AggregateOp.descriptor, RangeSource.descriptor)
    needing.foreach(d => withClue(d.kind)(d.config.validate(Json.obj()).nonEmpty || d.checkParams(Json.obj()).nonEmpty shouldBe true))
    succeed
  }
}
