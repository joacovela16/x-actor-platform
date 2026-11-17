package xplat.flow.engine

import cats.effect.{IO, Ref}
import io.circe.Json
import xplat.core.io.actor.IOActorSystem
import xplat.flow.model._
import xplat.flow.nodes.NodeType

import scala.concurrent.duration._

/** Traces made from the audit: a span per instance and per node, subflows under the node that called them, and the traceparent
  * a node sends on while it runs.
  */
class FlowTracingTest extends EngineSpec {

  /** A node that writes, as its output, the traceparent the tracing gives it while it runs. */
  private def seeing(tracing: FlowTracing[IO]): NodeType[IO] = new NodeType[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor("test.see", "Test", "See", defaultOutputs = _ => List(OutputDecl("tp", StatePath(List("tp")), DataType.AnyJson)))
    def run(input: NodeInput): IO[NodeOutput] =
      tracing.traceparent(input.instanceId, input.nodeId).map(tp => NodeOutput.ok(Map("tp" -> tp.fold(Json.Null)(Json.fromString))))
  }

  private def withTracing[A](f: (FlowEngine[IO], Probe, FlowTracing[IO], IO[List[FlowTracing.Span]]) => IO[A]): IO[A] =
    for {
      journal <- InMemoryJournal.make[IO]
      probe <- Probe.make
      spans <- Ref.of[IO, List[FlowTracing.Span]](Nil)
      tracing <- FlowTracing.make[IO](s => spans.update(_ ++ s))
      result <- IOActorSystem[IO]("tracing").use(system =>
        ActorFlowEngine.make[IO](system, journal, List(probe.step, seeing(tracing)), observer = Some(tracing)).flatMap(f(_, probe, tracing, spans.get))
      )
    } yield result

  private def awaitSpans(get: IO[List[FlowTracing.Span]], n: Int): IO[List[FlowTracing.Span]] =
    awaitUntil(get)(_.size >= n).timeout(5.seconds)

  "an instance is a span and each node a span under it, with its times, attributes and error" in
    withTracing { (engine, probe, _, spans) =>
      val f = flow("traced", List(step("a"), step("b")), List(edge("a", "b")))
      for {
        _ <- probe.breakDown("b")
        id <- run(engine, f)
        _ <- finished(engine, id)
        all <- awaitSpans(spans, 3)
      } yield {
        val root = all.find(_.kind == FlowTracing.Span.Server).get
        val nodes = all.filter(_.kind == FlowTracing.Span.Internal)
        root.name shouldBe "flow traced"
        root.parentSpanId shouldBe None
        root.attributes("flow.instance") shouldBe id
        root.attributes("flow.status") shouldBe "failed"
        root.error shouldBe Some("b: b is broken")
        nodes.map(_.name) shouldBe List("test.step a", "test.step b")
        nodes.map(_.traceId).distinct shouldBe List(root.traceId)
        nodes.map(_.parentSpanId) shouldBe List(Some(root.spanId), Some(root.spanId))
        nodes.find(_.attributes("flow.node") == "b").get.error shouldBe Some("b is broken")
        nodes.forall(n => n.startMs >= root.startMs && n.endMs <= root.endMs && n.startMs <= n.endMs) shouldBe true
        root.traceId should fullyMatch regex "[0-9a-f]{32}"
        root.spanId should fullyMatch regex "[0-9a-f]{16}"
      }
    }

  "a node knows its traceparent while it runs, and an instance linked to one goes on that trace" in
    withTracing { (engine, _, tracing, spans) =>
      val f = flow("see", List(NodeDef("s", "test.see", Json.obj(), List(OutputDecl("tp", path("tp"), DataType.AnyJson)))), Nil)
      val incoming = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
      for {
        _ <- save(engine, f)
        _ <- tracing.link("given-id", incoming)
        id <- engine.start("see", None, Json.obj(), Some("given-id"))
        status <- finished(engine, id)
        all <- awaitSpans(spans, 2)
        after <- tracing.traceparent(id, "s")
      } yield {
        val root = all.find(_.kind == FlowTracing.Span.Server).get
        val node = all.find(_.kind == FlowTracing.Span.Internal).get
        root.traceId shouldBe "0af7651916cd43dd8448eb211c80319c"
        root.parentSpanId shouldBe Some("b7ad6b7169203331")
        valueAt(status, "tp") shouldBe Some(Json.fromString(s"00-${node.traceId}-${node.spanId}-01"))
        after shouldBe None
      }
    }

  "a subflow goes on the trace of its caller, under the node that called it" in
    withTracing { (engine, _, _, spans) =>
      val child = flow("child", List(step("c", "value" -> Json.fromInt(1))), Nil, outputs = List(OutputDecl("r", path("out.c"), DataType.Num)))
      val parent = flow("parent", List(NodeDef("call", "core.subflow", json("""{"flowId":"child"}"""), List(OutputDecl("r", path("got"), DataType.Num)))), Nil)
      for {
        _ <- save(engine, child)
        id <- run(engine, parent)
        _ <- finished(engine, id)
        all <- awaitSpans(spans, 4)
      } yield {
        val call = all.find(_.name == "core.subflow call").get
        val childRoot = all.find(_.name == "flow child").get
        val parentRoot = all.find(_.name == "flow parent").get
        childRoot.traceId shouldBe parentRoot.traceId
        childRoot.parentSpanId shouldBe Some(call.spanId)
        call.attributes.get("flow.child_instance") shouldBe Some(childRoot.attributes("flow.instance"))
      }
    }

  "a traceparent that is not valid is ignored" in IO {
    FlowTracing.parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01") shouldBe Some(FlowTracing.Parent("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"))
    FlowTracing.parse("00-00000000000000000000000000000000-b7ad6b7169203331-01") shouldBe None
    FlowTracing.parse("garbage") shouldBe None
  }
}
