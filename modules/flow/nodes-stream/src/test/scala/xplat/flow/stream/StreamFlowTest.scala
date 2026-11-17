package xplat.flow.stream

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine.InstanceState.{NodeStatus, Status}
import xplat.flow.engine._
import xplat.flow.model._
import xplat.flow.nodes.{Connections, FlowRunner, NodeType}
import xplat.flow.queue.{ConsumerFactory, PublisherPool}

import scala.concurrent.duration._

/** Streams inside a real engine: instances that are actors, the status, the audit, retries, cancelling and simulation. */
class StreamFlowTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)
  private def path(text: String): StatePath = StatePath(text.split('.').toList)
  private def out(name: String, tpe: DataType = DataType.AnyJson) = OutputDecl(name, path(s"out.$name"), tpe)
  private def node(id: String, kind: String, params: Json = Json.obj(), outputs: List[OutputDecl] = Nil, policy: RunPolicy = RunPolicy.Always) = NodeDef(id, kind, params, outputs, policy)

  private val inputs = List(OutputDecl("items", path("items"), DataType.ListOf(DataType.AnyJson)), OutputDecl("ok", path("ok"), DataType.Bool))

  // ---- test nodes ----

  /** Counts how many times it was opened. */
  private final class Runs(runs: Ref[IO, Int]) extends StreamSource[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor("test.runs", "Test", "Runs", ports = _ => StreamPorts.output())
    def open(ctx: StreamContext[IO]): Stream[IO, Elem[IO]] =
      Stream.eval(runs.update(_ + 1) >> ctx.now).flatMap(t => Stream.emits(List(1, 2, 3).map(i => Elem.of[IO](Json.fromInt(i), t))))
  }

  private final class Forever(started: Deferred[IO, Unit], closed: Ref[IO, Boolean]) extends StreamSource[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor("test.forever", "Test", "Forever", ports = _ => StreamPorts.output())
    def open(ctx: StreamContext[IO]): Stream[IO, Elem[IO]] =
      Stream.bracket(started.complete(()))(_ => closed.set(true)).flatMap(_ => Stream.iterate(0)(_ + 1).covary[IO].metered(5.millis).evalMap(i => ctx.now.map(Elem.of[IO](Json.fromInt(i), _))))
  }

  private val noConnections: Connections[IO] = (_: String) => IO.pure(None)
  private val noPool: PublisherPool[IO] = new PublisherPool[IO] {
    def use[A](c: xplat.flow.nodes.Connections.Resolved, d: String, t: String)(f: xplat.core.connectors.Publisher[IO, Array[Byte]] => IO[A]): IO[A] = IO.raiseError(new IllegalStateException("no pool in this test"))
  }
  private val noConsumers: ConsumerFactory[IO] = (_, _, _) => throw new IllegalStateException("no consumers in this test")

  private def withEngine[A](extra: List[StreamNodeType[IO]] = Nil)(f: (FlowEngine[IO], SegmentMetrics[IO]) => IO[A]): IO[A] =
    for {
      journal <- InMemoryJournal.make[IO]
      metrics <- SegmentMetrics.inMemory[IO]
      nodes = (runner: FlowRunner[IO]) => {
        val all = StreamNodes.types[IO](noConnections, noConsumers, noPool, runner) ++ extra
        (new SegmentNode[IO](all.map(t => t.descriptor.kind -> t).toMap, metrics, SegmentNode.Settings(reportEvery = 20.millis)): NodeType[IO]) :: all.map(t => new StreamMember[IO](t): NodeType[IO])
      }
      result <- IOActorSystem[IO]("flows").use(system => ActorFlowEngine.make[IO](system, journal, runnerNodes = nodes, metrics = Some(metrics)).flatMap(f(_, metrics)))
    } yield result

  private def save(engine: FlowEngine[IO], flow: FlowDef): IO[FlowDef] =
    engine.store.save(flow).flatMap(r => IO.fromEither(r.left.map(e => new RuntimeException(e.map(_.message).mkString("; ")))))

  private def run(engine: FlowEngine[IO], flow: FlowDef, in: Json): IO[InstanceId] = save(engine, flow).flatMap(f => engine.start(f.id, None, in, None))
  private def finished(engine: FlowEngine[IO], id: InstanceId): IO[InstanceStatus] = engine.events(id).compile.drain >> engine.status(id).map(_.get)

  private def awaitUntil[A](io: IO[A])(p: A => Boolean): IO[A] = io.flatMap(a => if (p(a)) IO.pure(a) else IO.sleep(20.millis) >> awaitUntil(io)(p))

  // ---- a flow with a stream in the middle of it ----

  private def orders = FlowDef(
    "orders",
    "Orders",
    inputs = inputs,
    nodes = List(
      node("src", ListSource.Kind, json("""{"path": "items"}""")),
      node("big", FilterOp.Kind, json("""{"where": {"gt": [{"path": "total"}, 100]}}""")),
      node("calc", MapOp.Kind, json("""{"set": {"net": {"mul": [{"path": "total"}, 0.5]}}}""")),
      node("all", CollectSink.Kind, Json.obj(), List(out("big", DataType.ListOf(DataType.AnyJson)))),
      node("check", "core.if", json("""{"path": "ok"}""")),
      node("done", "core.if", json("""{"path": "ok"}"""))
    ),
    edges = List(Edge("src", "big"), Edge("big", "calc"), Edge("calc", "all"), Edge("all", "check"), Edge("check", "done", "then"))
  )

  private val items = json("""[{"id": 1, "total": 50}, {"id": 2, "total": 150}, {"id": 3, "total": 400}, {"id": "bad"}]""")

  "a stream is one step of the instance: the nodes before and after it run as usual" in withEngine() { (engine, _) =>
    for {
      id <- run(engine, orders, Json.obj("items" -> items, "ok" -> Json.True))
      s <- finished(engine, id)
      trail <- engine.audit(id).map(_.get)
    } yield {
      s.status shouldBe Status.Succeeded
      path("out.big").get(s.state).get.asArray.get.map(_.hcursor.get[Int]("net").toOption) shouldBe Vector(Some(75), Some(200))
      // The nodes of the stream are shown with the status of the segment, each with its metrics.
      List("src", "big", "calc", "all").foreach(n => s.nodes(n).status shouldBe NodeStatus.Succeeded)
      s.nodes("check").port shouldBe Some("then")
      s.nodes("done").status shouldBe NodeStatus.Succeeded
      s.nodes("big").metrics.get.hcursor.get[Long]("in") shouldBe Right(4L)
      s.nodes("all").metrics.get.hcursor.get[Long]("in") shouldBe Right(2L)
      // The element without `total` fails in the filter: it is a dead letter, which the status and the audit have.
      s.deadLetters.size shouldBe 1
      s.deadLetters.head.hcursor.get[String]("node") shouldBe Right("big")
      val finishedSegment = trail.entries.collect { case n: AuditEntry.NodeFinished if n.node == "src" => n }
      finishedSegment.size shouldBe 1
      finishedSegment.head.info.keySet should contain allOf ("metrics", "deadLetters")
      // There are no entries per element: the trail is as long as a flow of this many nodes would have.
      trail.entries.size should be < 30
    }
  }

  "the status as the API shows it has the metrics and the dead letters" in withEngine() { (engine, _) =>
    for {
      id <- run(engine, orders, Json.obj("items" -> items, "ok" -> Json.True))
      s <- finished(engine, id)
      shown = io.circe.Encoder[InstanceStatus].apply(s)
    } yield {
      shown.hcursor.downField("nodes").downField("big").downField("metrics").get[Long]("out") shouldBe Right(2L)
      shown.hcursor.get[List[Json]]("deadLetters").toOption.get.size shouldBe 1
      shown.hcursor.downField("nodes").downField("check").downField("metrics").focus shouldBe None
    }
  }

  "a flow with a mistake in its streams is not saved" in withEngine() { (engine, _) =>
    val tokenMix = orders.copy(edges = orders.edges :+ Edge("calc", "done"))
    val noSink = FlowDef("x", "X", nodes = List(node("src", ListSource.Kind, json("""{"path": "items"}""")), node("m", MapOp.Kind)), edges = List(Edge("src", "m")), inputs = inputs)
    for {
      a <- engine.store.save(tokenMix)
      b <- engine.store.save(noSink)
    } yield {
      a.left.toOption.get.map(_.code) should contain("stream-token-mix")
      b.left.toOption.get.map(_.code) shouldBe List("stream-without-sink")
    }
  }

  "an If before a stream decides which of its sources start" in withEngine() { (engine, _) =>
    val flow = FlowDef(
      "branches",
      "Branches",
      inputs = List(OutputDecl("ok", path("ok"), DataType.Bool)),
      nodes = List(
        node("gate", "core.if", json("""{"path": "ok"}""")),
        node("a", RangeSource.Kind, json("""{"from": 0, "to": 3}""")),
        node("b", RangeSource.Kind, json("""{"from": 100, "to": 102}""")),
        node("all", CollectSink.Kind, Json.obj(), List(out("all")))
      ),
      edges = List(Edge("gate", "a", "then"), Edge("gate", "b", "else"), Edge("a", "all"), Edge("b", "all"))
    )
    for {
      yes <- run(engine, flow, Json.obj("ok" -> Json.True)).flatMap(finished(engine, _))
      no <- engine.start("branches", None, Json.obj("ok" -> Json.False), None).flatMap(finished(engine, _))
    } yield {
      path("out.all").get(yes.state).get.asArray.get.map(_.asNumber.get.toInt.get).toSet shouldBe Set(0, 1, 2)
      path("out.all").get(no.state).get.asArray.get.map(_.asNumber.get.toInt.get).toSet shouldBe Set(100, 101)
      yes.status shouldBe Status.Succeeded
    }
  }

  "a stream that does not end runs until the instance is cancelled, and then it is closed" in {
    for {
      started <- Deferred[IO, Unit]
      closed <- Ref.of[IO, Boolean](false)
      flow = FlowDef("forever", "Forever", nodes = List(node("src", "test.forever"), node("all", CollectSink.Kind, Json.obj(), List(out("all")))), edges = List(Edge("src", "all")))
      r <- withEngine(List(new Forever(started, closed))) { (engine, metrics) =>
        for {
          id <- run(engine, flow, Json.obj())
          _ <- started.get.timeout(5.seconds)
          // While it runs, the status shows how it goes, live.
          live <- awaitUntil(engine.status(id).map(_.get))(s => s.nodes("src").metrics.exists(_.hcursor.get[Long]("out").exists(_ > 0)))
          _ <- engine.cancel(id)
          after <- engine.status(id).map(_.get)
          _ <- awaitUntil(closed.get)(identity)
          left <- metrics.snapshot(id)
          again <- engine.cancel(id) // an instance that has ended: nothing happens
        } yield (live, after, left, again)
      }
      (live, after, left, _) = r
    } yield {
      live.status shouldBe Status.Running
      live.nodes("src").status shouldBe NodeStatus.Running
      after.status shouldBe Status.Failed
      after.nodes("src").status shouldBe NodeStatus.Failed
      after.nodes("src").error shouldBe Some("cancelled")
      left shouldBe empty
    }
  }

  "a stream that was done is not run again by a retry if its run policy says so" in {
    for {
      runs <- Ref.of[IO, Int](0)
      flow = FlowDef(
        "once",
        "Once",
        nodes = List(node("src", "test.runs", policy = RunPolicy.OnceSucceeded), node("all", CollectSink.Kind, Json.obj(), List(out("all")))),
        edges = List(Edge("src", "all"))
      )
      r <- withEngine(List(new Runs(runs))) { (engine, _) =>
        for {
          id <- run(engine, flow, Json.obj())
          first <- finished(engine, id)
          _ <- engine.retry(id)
          second <- awaitUntil(engine.status(id).map(_.get))(s => s.status != Status.Running)
          trail <- engine.audit(id).map(_.get)
        } yield (first, second, trail)
      }
      (first, second, trail) = r
      n <- runs.get
    } yield {
      n shouldBe 1
      path("out.all").get(first.state).get shouldBe json("[1,2,3]")
      // The result of the first time is taken again.
      path("out.all").get(second.state).get shouldBe json("[1,2,3]")
      trail.entries.exists(e => e.kind == "node-skipped" && e.nodeOf.contains("src")) shouldBe true
    }
  }

  "the same stream is run again by a retry when its policy is to always run" in {
    for {
      runs <- Ref.of[IO, Int](0)
      flow = FlowDef("always", "Always", nodes = List(node("src", "test.runs"), node("all", CollectSink.Kind, Json.obj(), List(out("all")))), edges = List(Edge("src", "all")))
      _ <- withEngine(List(new Runs(runs))) { (engine, _) =>
        run(engine, flow, Json.obj()).flatMap(id => finished(engine, id) >> engine.retry(id) >> awaitUntil(runs.get)(_ == 2) >> awaitUntil(engine.status(id).map(_.get))(_.status != Status.Running))
      }
      n <- runs.get
    } yield n shouldBe 2
  }

  "a sink that runs a flow for each element starts one instance per element" in withEngine() { (engine, _) =>
    val child = FlowDef("child", "Child", inputs = List(OutputDecl("n", path("n"), DataType.Num)), nodes = Nil)
    val parent = FlowDef(
      "parent",
      "Parent",
      nodes = List(node("src", RangeSource.Kind, json("""{"to": 4}""")), node("each", FlowSink.Kind, json("""{"flowId": "child", "inputs": {"n": "$"}}"""), List(out("runs", DataType.Num)))),
      edges = List(Edge("src", "each"))
    )
    for {
      _ <- save(engine, child)
      id <- run(engine, parent, Json.obj())
      s <- finished(engine, id)
      kids <- engine.search(InstanceQuery(flowId = Some("child")))
    } yield {
      s.status shouldBe Status.Succeeded
      path("out.runs").get(s.state) shouldBe Some(Json.fromInt(4))
      kids.items.size shouldBe 4
      kids.items.foreach(_.origin shouldBe Origin.Subflow(id, "each")) // the node that runs the flow, not the step of the stream
    }
  }

  "a simulation runs what is pure, takes mocks for the sources and does not touch the outside" in withEngine() { (engine, _) =>
    val flow = FlowDef(
      "sim",
      "Sim",
      nodes = List(
        node("src", QueueSource.Kind, json("""{"connection": "kafka-prod", "source": "orders"}""")),
        node("calc", MapOp.Kind, json("""{"set": {"net": {"mul": [{"path": "total"}, 0.5]}}}""")),
        node("all", CollectSink.Kind, Json.obj(), List(out("all"))),
        node("pub", PublishSink.Kind, json("""{"connection": "kafka-prod", "destination": "audit"}"""), List(out("sent", DataType.Num)))
      ),
      edges = List(Edge("src", "calc"), Edge("calc", "all"), Edge("calc", "pub"))
    )
    val mocks = Map("src" -> Mock.Output(Map("elements" -> json("""[{"total": 10}, {"total": 30}]"""))))
    for {
      trace <- engine.simulate(flow, Json.obj(), mocks)
    } yield {
      trace.status shouldBe Status.Succeeded
      path("out.all").get(trace.state).get.asArray.get.map(_.hcursor.get[Int]("net").toOption) shouldBe Vector(Some(5), Some(15))
      val step = trace.steps.find(_.nodeId == "src").get
      step.kind shouldBe StreamSegments.SegmentKind
      step.outcome shouldBe "simulated"
      // What the publisher would have sent is in the samples of its node.
      step.info("metrics").hcursor.downField("pub").get[List[Json]]("sampleIn").toOption.get.size shouldBe 2
      io.circe.Encoder[Trace].apply(trace).hcursor.downField("audit").focus.get.noSpaces should include("simulated")
    }
  }
}
