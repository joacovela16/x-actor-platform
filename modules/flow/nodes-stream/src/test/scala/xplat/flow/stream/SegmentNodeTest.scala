package xplat.flow.stream

import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.engine.SegmentMetrics
import xplat.flow.model._
import xplat.flow.nodes.{CoreNodes, Connections, FlowRunner}
import xplat.flow.queue.{ConsumerFactory, PublisherPool}

import scala.concurrent.duration._

/** A segment run on its own, without an instance: the way the nodes are compiled into one stream. */
class SegmentNodeTest extends StreamSpec {

  // ---- nodes made for the tests ----

  private def sourceDescriptor(kind: String) = NodeDescriptor(kind, "Test", kind, ports = _ => StreamPorts.output())

  /** Gives 1..total, counting how many it has produced and how many were acknowledged. */
  private final class Counting(emitted: Ref[IO, Int], acks: Ref[IO, Int], total: Int) extends StreamSource[IO] {
    val descriptor: NodeDescriptor = sourceDescriptor("test.counting")
    def open(ctx: StreamContext[IO]): Stream[IO, Elem[IO]] =
      Stream.range(1, total + 1).evalMap(i => emitted.update(_ + 1) >> ctx.now.map(t => Elem[IO](Json.fromInt(i), t, acks.update(_ + 1))))
  }

  /** Never ends; says when it started and when it was closed. */
  private final class Forever(started: Deferred[IO, Unit], closed: Ref[IO, Boolean]) extends StreamSource[IO] {
    val descriptor: NodeDescriptor = sourceDescriptor("test.forever")
    def open(ctx: StreamContext[IO]): Stream[IO, Elem[IO]] =
      Stream.bracket(started.complete(()))(_ => closed.set(true)).flatMap(_ => Stream.iterate(0)(_ + 1).covary[IO].metered(5.millis).evalMap(i => ctx.now.map(Elem.of[IO](Json.fromInt(i), _))))
  }

  /** Takes `delay` for each element. With a gate it waits for it first. */
  private final class Slow(delay: FiniteDuration, seen: Ref[IO, Int], gate: Option[Deferred[IO, Unit]] = None) extends StreamSink[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor("test.slow", "Test", "Slow", inputs = _ => StreamPorts.input)
    def drain(ctx: StreamContext[IO], upstream: Stream[IO, Elem[IO]]): IO[Map[String, Json]] =
      upstream.evalMap(e => gate.traverse_(_.get) >> IO.sleep(delay) >> seen.update(_ + 1) >> e.ack).compile.drain >> seen.get.map(n => ctx.result(Json.fromInt(n)))
  }

  /** Fails with an exception on the element 3. */
  private final class Boom extends StreamOperator[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor("test.boom", "Test", "Boom", ports = _ => StreamPorts.output(), inputs = _ => StreamPorts.input)
    def apply(ctx: StreamContext[IO], inputs: Map[String, Stream[IO, Elem[IO]]]): Stream[IO, Elem[IO]] =
      inputs(Port.In).evalMap(e => if (e.value == Json.fromInt(3)) IO.raiseError(new RuntimeException("boom at 3")) else IO.pure(e))
  }

  // ---- building and running ----

  private val noConnections: Connections[IO] = (_: String) => IO.pure(None)
  private val noPool: PublisherPool[IO] = new PublisherPool[IO] {
    def use[A](c: xplat.flow.nodes.Connections.Resolved, d: String, t: String)(f: xplat.core.connectors.Publisher[IO, Array[Byte]] => IO[A]): IO[A] = IO.raiseError(new IllegalStateException("no pool in this test"))
  }
  private val noConsumers: ConsumerFactory[IO] = (_, _, _) => throw new IllegalStateException("no consumers in this test")
  private val noRunner: FlowRunner[IO] = (_, _, _, _, _) => IO.pure(FlowRunner.Finished(Map.empty))

  private def out(name: String, tpe: DataType = DataType.AnyJson) = OutputDecl(name, path(s"out.$name"), tpe)
  private def node(id: String, kind: String, params: Json = Json.obj(), outputs: List[OutputDecl] = Nil) = NodeDef(id, kind, params, outputs)

  private final class Rig(extra: List[StreamNodeType[IO]], settings: SegmentNode.Settings, val metrics: SegmentMetrics[IO]) {
    val types: List[StreamNodeType[IO]] = StreamNodes.types[IO](noConnections, noConsumers, noPool, noRunner) ++ extra
    val catalog: NodeCatalog = StreamNodes.catalog ++ CoreNodes.catalog ++ new NodeCatalog(extra.map(_.descriptor))
    val segment = new SegmentNode[IO](types.map(t => t.descriptor.kind -> t).toMap, metrics, settings)

    def plan(flow: FlowDef): NodeDef = StreamSegments.condense(flow, catalog).nodes.find(_.kind == StreamSegments.SegmentKind).get

    def run(flow: FlowDef, state: Json = Json.obj(), fired: List[String] = Nil, simulation: Option[Json] = None): IO[NodeOutput] = {
      val p = plan(flow)
      val params = simulation.fold(p.params)(s => p.params.mapObject(_.add("simulation", s)))
      segment.run(NodeInput(params, state, instanceId = "inst", nodeId = p.id, firedInputs = fired))
    }
  }

  private def rig(extra: List[StreamNodeType[IO]] = Nil, settings: SegmentNode.Settings = SegmentNode.Settings(reportEvery = 20.millis)): IO[Rig] =
    SegmentMetrics.inMemory[IO].map(new Rig(extra, settings, _))

  private def metricsOf(o: NodeOutput, id: String): Json = o.info("metrics").hcursor.downField(id).focus.get
  private def count(j: Json, field: String): Long = j.hcursor.get[Long](field).toOption.get

  private def flow(nodes: List[NodeDef], edges: List[Edge], inputs: List[OutputDecl] = Nil): FlowDef = FlowDef("f", "F", nodes, edges, inputs)

  // ---- the tests ----

  "a pipeline of a list, a map, a filter, a window, an aggregate and a collect runs as one step" in {
    val f = flow(
      List(
        node("src", ListSource.Kind, json("""{"path": "orders"}""")),
        node("map", MapOp.Kind, json("""{"set": {"big": {"gt": [{"path": "total"}, 100]}}}""")),
        node("keep", FilterOp.Kind, json("""{"where": {"path": "big"}}""")),
        node("win", WindowOp.Kind, json("""{"size": 2}""")),
        node("agg", AggregateOp.Kind, json("""{"over": "items", "key": "shop", "aggregations": {"n": {"op": "count"}, "sum": {"op": "sum", "path": "total"}}}""")),
        node("all", CollectSink.Kind, Json.obj(), List(out("rows")))
      ),
      List(Edge("src", "map"), Edge("map", "keep"), Edge("keep", "win"), Edge("win", "agg"), Edge("agg", "all"))
    )
    val state = json("""{"orders": [{"shop":"a","total":50},{"shop":"a","total":150},{"shop":"b","total":200},{"shop":"a","total":300},{"shop":"b","total":10}]}""")
    for {
      r <- rig()
      o <- r.run(f, state)
    } yield {
      o.failed shouldBe false
      o.port shouldBe Port.Out
      o.values.keySet shouldBe Set("all.rows")
      // 3 pass the filter (150, 200, 300): windows of two => [a150, b200] and [a300].
      o.values("all.rows") shouldBe json("""[{"key":"a","n":1,"sum":150},{"key":"b","n":1,"sum":200},{"key":"a","n":1,"sum":300}]""")
      count(metricsOf(o, "src"), "out") shouldBe 5
      count(metricsOf(o, "keep"), "in") shouldBe 5
      count(metricsOf(o, "keep"), "out") shouldBe 3
      count(metricsOf(o, "keep"), "dropped") shouldBe 2
      count(metricsOf(o, "win"), "out") shouldBe 2
      count(metricsOf(o, "all"), "in") shouldBe 3
      count(metricsOf(o, "all"), "out") shouldBe 3 // a sink: what it delivered
      metricsOf(o, "src").hcursor.get[List[Json]]("sampleOut").toOption.get.size shouldBe 5
      o.info("deadLetters") shouldBe json("[]")
    }
  }

  "a branch sends each element through then or else, and the next nodes see it as an ordinary element" in {
    val f = flow(
      List(
        node("src", ListSource.Kind, json("""{"path": "orders"}""")),
        node("big", BranchOp.Kind, json("""{"where": {"gt": [{"path": "total"}, 100]}}""")),
        node("tag", MapOp.Kind, json("""{"set": {"big": true}}""")),
        node("large", CollectSink.Kind, Json.obj(), List(out("rows"))),
        node("small", CollectSink.Kind, Json.obj(), List(out("rows")))
      ),
      List(Edge("src", "big"), Edge("big", "tag", BranchOp.Then), Edge("tag", "large"), Edge("big", "small", BranchOp.Else))
    )
    val state = json("""{"orders": [{"total":50},{"total":150},{"total":200},{"total":10}]}""")
    for {
      r <- rig()
      o <- r.run(f, state)
    } yield {
      o.failed shouldBe false
      o.values("large.rows") shouldBe json("""[{"total":150,"big":true},{"total":200,"big":true}]""")
      o.values("small.rows") shouldBe json("""[{"total":50},{"total":10}]""")
      count(metricsOf(o, "big"), "out") shouldBe 4
    }
  }

  "a branch with nothing connected to one of its ports lets those elements go" in {
    val f = flow(
      List(
        node("src", ListSource.Kind, json("""{"path": "orders"}""")),
        node("big", BranchOp.Kind, json("""{"where": {"gt": [{"path": "total"}, 100]}}""")),
        node("large", CollectSink.Kind, Json.obj(), List(out("rows")))
      ),
      List(Edge("src", "big"), Edge("big", "large", BranchOp.Then))
    )
    for {
      r <- rig()
      o <- r.run(f, json("""{"orders": [{"total":50},{"total":150},{"total":10}]}"""))
    } yield {
      o.failed shouldBe false
      o.values("large.rows") shouldBe json("""[{"total":150}]""")
      o.info("deadLetters") shouldBe json("[]")
    }
  }

  /** Runs `src (list of `items`) -> op -> collect` and answers what was collected. */
  private def through(kind: String, params: String, items: String): IO[(NodeOutput, Json)] = {
    val f = flow(
      List(node("src", ListSource.Kind, json("""{"path": "items"}""")), node("op", kind, json(params)), node("all", CollectSink.Kind, Json.obj(), List(out("rows")))),
      List(Edge("src", "op"), Edge("op", "all"))
    )
    rig().flatMap(_.run(f, json(s"""{"items": $items}"""))).map(o => o -> o.values.getOrElse("all.rows", Json.Null))
  }

  "take, drop, takeWhile and dropWhile cut the stream as in fs2" in {
    for {
      take <- through(TakeOp.Kind, """{"n": 2}""", "[1,2,3,4]")
      drop <- through(DropOp.Kind, """{"n": 3}""", "[1,2,3,4]")
      tw <- through(TakeWhileOp.Kind, """{"where": {"lt": [{"path": "x"}, 3]}}""", """[{"x":1},{"x":2},{"x":5},{"x":1}]""")
      dw <- through(DropWhileOp.Kind, """{"where": {"lt": [{"path": "x"}, 3]}}""", """[{"x":1},{"x":2},{"x":5},{"x":1}]""")
    } yield {
      take._2 shouldBe json("[1,2]")
      drop._2 shouldBe json("[4]")
      tw._2 shouldBe json("""[{"x":1},{"x":2}]""")
      dw._2 shouldBe json("""[{"x":5},{"x":1}]""")
      List(take, drop, tw, dw).foreach(_._1.failed shouldBe false)
    }
  }

  "scan, changes and groupAdjacentBy keep what they need between elements" in {
    for {
      scan <- through(ScanOp.Kind, """{"init": 0, "step": {"add": [{"path": "acc"}, {"path": "elem.n"}]}}""", """[{"n":1},{"n":2},{"n":3}]""")
      changes <- through(ChangesOp.Kind, """{"key": "s"}""", """[{"s":"a","i":1},{"s":"a","i":2},{"s":"b","i":3},{"s":"a","i":4}]""")
      groups <- through(GroupAdjacentByOp.Kind, """{"key": "s"}""", """[{"s":"a"},{"s":"a"},{"s":"b"}]""")
    } yield {
      scan._2 shouldBe json("[1,3,6]")
      changes._2 shouldBe json("""[{"s":"a","i":1},{"s":"b","i":3},{"s":"a","i":4}]""")
      groups._2 shouldBe json("""[{"key":"a","items":[{"s":"a"},{"s":"a"}],"count":2},{"key":"b","items":[{"s":"b"}],"count":1}]""")
    }
  }

  "zipWithIndex, zipWithPrevious and zipWithNext pair each element as in fs2" in {
    for {
      idx <- through(ZipWithIndexOp.Kind, "{}", """["a","b"]""")
      prev <- through(ZipWithPreviousOp.Kind, "{}", """["a","b"]""")
      next <- through(ZipWithNextOp.Kind, "{}", """["a","b"]""")
    } yield {
      idx._2 shouldBe json("""[{"value":"a","index":0},{"value":"b","index":1}]""")
      prev._2 shouldBe json("""[{"previous":null,"value":"a"},{"previous":"a","value":"b"}]""")
      next._2 shouldBe json("""[{"value":"a","next":"b"},{"value":"b","next":null}]""")
    }
  }

  "a take after a source that never ends stops it, and the segment ends" in {
    for {
      started <- Deferred[IO, Unit]
      closed <- Ref.of[IO, Boolean](false)
      r <- rig(List(new Forever(started, closed)))
      f = flow(
        List(node("src", "test.forever"), node("first", TakeOp.Kind, json("""{"n": 3}""")), node("all", CollectSink.Kind, Json.obj(), List(out("rows")))),
        List(Edge("src", "first"), Edge("first", "all"))
      )
      o <- r.run(f).timeout(10.seconds)
      c <- closed.get
    } yield {
      o.failed shouldBe false
      o.values("all.rows") shouldBe json("[0,1,2]")
      c shouldBe true
    }
  }

  "a take stops what is before it through every node in between, and acknowledges what was on its way" in {
    for {
      emitted <- Ref.of[IO, Int](0)
      acks <- Ref.of[IO, Int](0)
      r <- rig(List(new Counting(emitted, acks, 1000)))
      f = flow(
        List(node("src", "test.counting"), node("tag", ZipWithIndexOp.Kind), node("first", TakeOp.Kind, json("""{"n": 2}""")), node("all", CollectSink.Kind, Json.obj(), List(out("rows")))),
        List(Edge("src", "tag"), Edge("tag", "first"), Edge("first", "all"))
      )
      o <- r.run(f).timeout(10.seconds)
      e <- emitted.get
      n <- acks.get
    } yield {
      o.failed shouldBe false
      o.values("all.rows").asArray.get.size shouldBe 2
      e should be < 1000
      // Everything that left the source is acknowledged. The source may have made one more element just as it was stopped, which
      // never left it: from a broker, that message is not acknowledged and comes again (at least once).
      n should (be >= e - 1 and be <= e)
    }
  }

  "elements that cannot be handled are dead letters, in the answer, and the segment still succeeds" in {
    val f = flow(
      List(node("src", ListSource.Kind, json("""{"path": "xs"}""")), node("map", MapOp.Kind, json("""{"set": {"y": {"path": "x"}}}""")), node("all", CollectSink.Kind, Json.obj(), List(out("ys")))),
      List(Edge("src", "map"), Edge("map", "all"))
    )
    for {
      r <- rig()
      o <- r.run(f, json("""{"xs": [{"x": 1}, {"z": 2}, {"x": 3}]}"""))
    } yield {
      o.failed shouldBe false
      o.values("all.ys").asArray.get.size shouldBe 2
      val letters = o.info("deadLetters").asArray.get
      letters.size shouldBe 1
      letters.head.hcursor.get[String]("node") shouldBe Right("map")
      letters.head.hcursor.get[Json]("element") shouldBe Right(json("""{"z":2}"""))
      count(metricsOf(o, "map"), "failed") shouldBe 1
    }
  }

  "a port with several edges broadcasts to all of them, and every element is acknowledged once, after all are done with it" in {
    for {
      emitted <- Ref.of[IO, Int](0)
      acks <- Ref.of[IO, Int](0)
      r <- rig(List(new Counting(emitted, acks, 20)))
      f = flow(
        List(node("src", "test.counting"), node("keep", FilterOp.Kind, json("""{"where": {"gt": [{"path": "x"}, 0]}}""")).copy(), node("all", CollectSink.Kind, Json.obj(), List(out("all"))), node("cnt", ReduceSink.Kind, Json.obj(), List(out("cnt"))), node("sum", ReduceSink.Kind, json("""{"op": "sum", "path": "$"}""")).copy()),
        Nil
      )
      // src -> all, src -> cnt: both sinks see all 20.
      g = flow(
        List(node("src", "test.counting"), node("all", CollectSink.Kind, Json.obj(), List(out("all"))), node("cnt", ReduceSink.Kind, Json.obj(), List(out("cnt")))),
        List(Edge("src", "all"), Edge("src", "cnt"))
      )
      o <- r.run(g)
      n <- acks.get
    } yield {
      o.values("all.all").asArray.get.size shouldBe 20
      o.values("cnt.cnt") shouldBe json("20")
      n shouldBe 20
      f.nodes.size should be > 0
    }
  }

  "everything is acknowledged, also what was dropped or failed, once the segment is done" in {
    for {
      emitted <- Ref.of[IO, Int](0)
      acks <- Ref.of[IO, Int](0)
      r <- rig(List(new Counting(emitted, acks, 10)))
      f = flow(
        List(node("src", "test.counting"), node("keep", FilterOp.Kind, json("""{"where": {"gt": [{"path": "$"}, 5]}}""")), node("all", CollectSink.Kind, Json.obj(), List(out("all")))),
        List(Edge("src", "keep"), Edge("keep", "all"))
      )
      o <- r.run(f)
      n <- acks.get
    } yield {
      // `$` is not a field: every element fails the condition (dead letters) and is acknowledged.
      o.failed shouldBe false
      o.info("deadLetters").asArray.get.size shouldBe 10
      n shouldBe 10
    }
  }

  "an element is not acknowledged until the sink has dealt with it (at least once)" in {
    for {
      emitted <- Ref.of[IO, Int](0)
      acks <- Ref.of[IO, Int](0)
      seen <- Ref.of[IO, Int](0)
      gate <- Deferred[IO, Unit]
      r <- rig(List(new Counting(emitted, acks, 3), new Slow(Duration.Zero, seen, Some(gate))))
      f = flow(List(node("src", "test.counting"), node("end", "test.slow", Json.obj(), List(out("n")))), List(Edge("src", "end")))
      running <- r.run(f).start
      _ <- IO.sleep(100.millis)
      before <- acks.get
      _ <- gate.complete(())
      o <- running.joinWithNever
      after <- acks.get
    } yield {
      before shouldBe 0
      after shouldBe 3
      o.values("end.n") shouldBe json("3")
    }
  }

  "a slow sink holds back the source: it does not run ahead (backpressure)" in {
    for {
      emitted <- Ref.of[IO, Int](0)
      acks <- Ref.of[IO, Int](0)
      seen <- Ref.of[IO, Int](0)
      r <- rig(List(new Counting(emitted, acks, 1000), new Slow(100.millis, seen)), SegmentNode.Settings(capacity = 2, reportEvery = 1.hour))
      f = flow(List(node("src", "test.counting"), node("keep", MapOp.Kind, json("{}")), node("end", "test.slow", Json.obj(), List(out("n")))), List(Edge("src", "keep"), Edge("keep", "end")))
      control <- TestControl.execute(r.run(f))
      _ <- control.tick
      // The clock jumps by what it is told, so it moves in steps as short as what the sink takes.
      _ <- (1 to 10).toList.traverse_(_ => control.advanceAndTick(100.millis))
      consumed <- seen.get
      produced <- emitted.get
    } yield {
      consumed should be >= 9
      // What is in flight is bounded by the queues between nodes, not by the 1000 that the source could give.
      produced should be <= consumed + 30
      produced should be < 100
    }
  }

  "a node that fails stops the whole segment, which fails through the error port with what it counted" in {
    val f = flow(
      List(node("src", RangeSource.Kind, json("""{"to": 10}""")), node("boom", "test.boom"), node("all", CollectSink.Kind, Json.obj(), List(out("all")))),
      List(Edge("src", "boom"), Edge("boom", "all"))
    )
    for {
      r <- rig(List(new Boom))
      o <- r.run(f).timeout(5.seconds)
    } yield {
      o.failed shouldBe true
      o.port shouldBe Port.Error
      o.error.get should include("boom at 3")
      o.info.keySet should contain("metrics")
    }
  }

  "stopping a segment that never ends closes its sources" in {
    for {
      started <- Deferred[IO, Unit]
      closed <- Ref.of[IO, Boolean](false)
      r <- rig(List(new Forever(started, closed)))
      f = flow(List(node("src", "test.forever"), node("all", CollectSink.Kind, Json.obj(), List(out("all")))), List(Edge("src", "all")))
      running <- r.run(f).start
      _ <- started.get.timeout(5.seconds)
      _ <- IO.sleep(100.millis)
      live <- r.metrics.snapshot("inst")
      _ <- running.cancel
      _ <- IO.sleep(50.millis)
      isClosed <- closed.get
      after <- r.metrics.snapshot("inst")
    } yield {
      isClosed shouldBe true
      // While it ran it reported how it went, and when it stopped the report was dropped.
      live.keySet should have size 1
      count(live.values.head.hcursor.downField("nodes").downField("src").focus.get, "out") should be > 0L
      after shouldBe empty
    }
  }

  "a source starts only if the edge that leads to it fired" in {
    val f = flow(
      List(
        node("t", "core.if", json("""{"path": "x"}""")),
        node("a", ListSource.Kind, json("""{"path": "as"}""")),
        node("b", ListSource.Kind, json("""{"path": "bs"}""")),
        node("all", CollectSink.Kind, Json.obj(), List(out("all")))
      ),
      List(Edge("t", "a", "then"), Edge("t", "b", "else"), Edge("a", "all"), Edge("b", "all"))
    )
    val state = json("""{"as": [1, 2], "bs": [10, 20, 30]}""")
    for {
      r <- rig()
      onlyA <- r.run(f, state, fired = List("a"))
      onlyB <- r.run(f, state, fired = List("b"))
      both <- r.run(f, state, fired = List("a", "b"))
    } yield {
      onlyA.values("all.all").asArray.get.map(_.asNumber.get.toInt.get).toSet shouldBe Set(1, 2)
      onlyB.values("all.all").asArray.get.map(_.asNumber.get.toInt.get).toSet shouldBe Set(10, 20, 30)
      both.values("all.all").asArray.get.size shouldBe 5
    }
  }

  "two sources merge, zip and join inside the segment" in {
    val sources = List(node("l", ListSource.Kind, json("""{"path": "ls"}""")), node("r", ListSource.Kind, json("""{"path": "rs"}""")))
    val state = json("""{"ls": [{"id": 1, "a": "x"}, {"id": 2, "a": "y"}], "rs": [{"rid": 2, "b": "Y"}, {"rid": 1, "b": "X"}]}""")
    for {
      r <- rig()
      merged <- r.run(flow(sources ++ List(node("m", MergeOp.Kind), node("all", CollectSink.Kind, Json.obj(), List(out("all")))), List(Edge("l", "m"), Edge("r", "m"), Edge("m", "all"))), state)
      zipped <- r.run(flow(sources ++ List(node("z", ZipOp.Kind), node("all", CollectSink.Kind, Json.obj(), List(out("all")))), List(Edge("l", "z", input = "left"), Edge("r", "z", input = "right"), Edge("z", "all"))), state)
      joined <- r.run(flow(sources ++ List(node("j", JoinOp.Kind, json("""{"leftKey": "id", "rightKey": "rid"}""")), node("all", CollectSink.Kind, Json.obj(), List(out("all")))), List(Edge("l", "j", input = "left"), Edge("r", "j", input = "right"), Edge("j", "all"))), state)
    } yield {
      merged.values("all.all").asArray.get.size shouldBe 4
      zipped.values("all.all").asArray.get.map(v => v.hcursor.downField("left").get[Int]("id").toOption.get -> v.hcursor.downField("right").get[Int]("rid").toOption.get) shouldBe Vector(1 -> 2, 2 -> 1)
      joined.values("all.all").asArray.get.map(v => v.hcursor.downField("left").get[String]("a").toOption.get + v.hcursor.downField("right").get[String]("b").toOption.get).toSet shouldBe Set("xX", "yY")
    }
  }

  "in a simulation, mocks give the elements of a source, effects outside are not done, and what is pure runs" in {
    val f = flow(
      List(
        node("src", QueueSource.Kind, json("""{"connection": "kafka-prod", "source": "orders"}""")),
        node("map", MapOp.Kind, json("""{"set": {"double": {"mul": [{"path": "n"}, 2]}}}""")),
        node("all", CollectSink.Kind, Json.obj(), List(out("all"))),
        node("pub", PublishSink.Kind, json("""{"connection": "kafka-prod", "destination": "out"}"""), List(out("sent")))
      ),
      List(Edge("src", "map"), Edge("map", "all"), Edge("map", "pub"))
    )
    val simulation = json("""{"mocks": {"src": {"elements": [{"n": 1}, {"n": 2}, {"n": 3}]}}}""")
    for {
      r <- rig()
      o <- r.run(f, simulation = Some(simulation))
      noMock <- r.run(f, simulation = Some(json("""{"mocks": {}}""")))
      failing <- r.run(f, simulation = Some(json("""{"mocks": {"src": {"error": "broker is down"}}}""")))
    } yield {
      o.failed shouldBe false
      o.values("all.all").asArray.get.map(_.hcursor.get[Int]("double").toOption.get) shouldBe Vector(2, 4, 6)
      // The publisher was not used (the pool of this test would have failed), and its output has the default of its type.
      o.values("pub.sent") shouldBe Json.Null
      count(metricsOf(o, "pub"), "in") shouldBe 3
      metricsOf(o, "pub").hcursor.get[List[Json]]("sampleIn").toOption.get.size shouldBe 3
      // A queue that has no mock gives nothing in a simulation, because it reads from outside.
      noMock.values("all.all") shouldBe json("[]")
      failing.failed shouldBe true
      failing.error.get should include("broker is down")
    }
  }

  "a segment whose node is of a kind that is not known fails instead of throwing" in {
    val f = flow(List(node("src", RangeSource.Kind, json("""{"to": 2}""")), node("all", CollectSink.Kind, Json.obj(), List(out("all")))), List(Edge("src", "all")))
    for {
      r <- rig()
      p = r.plan(f)
      broken = p.params.mapObject(_.add("members", Json.arr(StreamSegments.membersOf(p.params).map(m => io.circe.Encoder[NodeDef].apply(m.copy(kind = "stream.nope"))): _*)))
      o <- r.segment.run(NodeInput(broken, Json.obj(), instanceId = "i", nodeId = p.id))
      bad <- r.segment.run(NodeInput(Json.obj(), Json.obj()))
    } yield {
      o.error.get should include("stream.nope")
      bad.failed shouldBe true
    }
  }
}
