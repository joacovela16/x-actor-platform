package xplat.flow.model

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class StreamSegmentsTest extends AnyFreeSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)
  private def path(text: String): StatePath = StatePath.parse(text).toOption.get

  private val elements = DataType.Num

  private val source = NodeDescriptor("t.source", "Stream", "Source", ports = _ => List(Port("out", stream = Some(elements))))
  private val operator = NodeDescriptor(
    "t.op", "Stream", "Operator",
    ports = _ => List(Port("out", stream = Some(elements))),
    inputs = _ => List(Port("in", stream = Some(elements)))
  )
  private val textOperator = NodeDescriptor(
    "t.text", "Stream", "Text",
    ports = _ => List(Port("out", stream = Some(DataType.Str))),
    inputs = _ => List(Port("in", stream = Some(DataType.Str)))
  )
  private val join = NodeDescriptor(
    "t.join", "Stream", "Join",
    ports = _ => List(Port("out", stream = Some(elements))),
    inputs = _ => List(Port("left", stream = Some(elements)), Port("right", stream = Some(elements)))
  )
  private val sink = NodeDescriptor("t.sink", "Stream", "Sink", inputs = _ => List(Port("in", stream = Some(elements))))
  private val plain = NodeDescriptor("t.plain", "Test", "Plain")
  private val catalog = new NodeCatalog(List(source, operator, textOperator, join, sink, plain))

  private def n(id: String, kind: String, outputs: List[OutputDecl] = Nil) = NodeDef(id, kind, outputs = outputs)
  private def codes(flow: FlowDef): List[String] = FlowValidator.validate(flow, catalog).map(_.code)

  // before -> (a, b sources) -> op -> sink -> after
  private val flow = FlowDef(
    "f", "F",
    nodes = List(n("before", "t.plain"), n("b", "t.source"), n("a", "t.source"), n("op", "t.op"), n("sink", "t.sink", List(OutputDecl("total", path("total"), DataType.Num))), n("after", "t.plain")),
    edges = List(Edge("before", "a"), Edge("a", "op"), Edge("b", "op"), Edge("op", "sink"), Edge("sink", "after"))
  )

  "ports and edges keep their JSON when there is no stream, and carry it when there is" in {
    json("""{"name":"out"}""").as[Port] shouldBe Right(Port("out"))
    Port("out").asJson shouldBe json("""{"name":"out"}""")
    Port("out", stream = Some(DataType.Num)).asJson shouldBe json("""{"name":"out","stream":{"type":"num"}}""")
    json("""{"from":"a","to":"b"}""").as[Edge] shouldBe Right(Edge("a", "b"))
    Edge("a", "b").asJson shouldBe json("""{"from":"a","port":"out","to":"b"}""")
    Edge("a", "b", input = "left").asJson shouldBe json("""{"from":"a","port":"out","to":"b","input":"left"}""")
    json("""{"from":"a","to":"b","input":"right"}""").as[Edge] shouldBe Right(Edge("a", "b", input = "right"))
  }

  "a flow with a stream is valid" in {
    FlowValidator.validate(flow, catalog) shouldBe Nil
  }

  "the segments are found, with the first source as their anchor" in {
    StreamSegments.find(flow, catalog) shouldBe List(StreamSegments.Segment("a", List("a", "b", "op", "sink"), List("a", "b"), List("sink")))
    StreamSegments.find(FlowDef("f", "F", List(n("x", "t.plain"))), catalog) shouldBe Nil
  }

  "a segment is condensed into one node, with its edges attached to the sources and the sinks" in {
    val plan = StreamSegments.condense(flow, catalog)
    plan.nodes.map(_.id) shouldBe List("before", "a", "after")
    val segment = plan.nodes.find(_.id == "a").get
    segment.kind shouldBe StreamSegments.SegmentKind
    segment.outputs.map(_.name) shouldBe List("sink.total")
    StreamSegments.membersOf(segment.params).map(_.id) shouldBe List("a", "b", "op", "sink")
    plan.edges.toSet shouldBe Set(Edge("before", "a", "out", "a"), Edge("a", "after", "out", "in"))
    segment.params.hcursor.get[List[String]]("entries") shouldBe Right(List("a"))
    segment.params.hcursor.get[List[Edge]]("edges").map(_.toSet) shouldBe Right(Set(Edge("a", "op"), Edge("b", "op"), Edge("op", "sink")))
  }

  "a flow without streams is its own plan" in {
    val plain = FlowDef("f", "F", List(n("x", "t.plain"), n("y", "t.plain")), List(Edge("x", "y")))
    StreamSegments.condense(plain, catalog) shouldBe plain
  }

  "the condensed node can be validated like any other: it reads what its members read" in {
    val reading = NodeDescriptor("t.reading", "Stream", "Reading", ports = _ => List(Port("out", stream = Some(elements))), reads = _ => List(StateRead(path("items"), DataType.AnyJson)))
    val c = new NodeCatalog(List(reading, sink))
    val f = FlowDef("f", "F", List(n("r", "t.reading"), n("s", "t.sink")), List(Edge("r", "s")))
    StreamSegments.membersOf(StreamSegments.condense(f, c).nodes.head.params).size shouldBe 2
    StreamSegments.condense(f, c).nodes.head.params.hcursor.get[List[StatePath]]("reads") shouldBe Right(List(path("items")))
  }

  "a stream cannot be joined to an ordinary edge" in {
    val intoToken = flow.copy(edges = flow.edges :+ Edge("op", "after"))
    codes(intoToken) shouldBe List("stream-token-mix")
    val fromToken = flow.copy(edges = flow.edges :+ Edge("before", "op"))
    codes(fromToken) shouldBe List("stream-token-mix")
  }

  "the elements of the two ends of a stream edge must be compatible" in {
    val f = FlowDef("f", "F", List(n("a", "t.source"), n("t", "t.text"), n("s", "t.sink")), List(Edge("a", "t"), Edge("t", "s")))
    FlowValidator.validate(f, catalog) shouldBe List(
      FlowError.StreamTypeMismatch(Edge("a", "t"), DataType.Str, DataType.Num),
      FlowError.StreamTypeMismatch(Edge("t", "s"), DataType.Num, DataType.Str)
    )
  }

  "an edge enters an input that exists" in {
    val f = flow.copy(edges = flow.edges :+ Edge("before", "after", input = "left"))
    codes(f) shouldBe List("unknown-input")
    val j = FlowDef("f", "F", List(n("a", "t.source"), n("b", "t.source"), n("j", "t.join"), n("s", "t.sink")), List(Edge("a", "j", input = "left"), Edge("b", "j", input = "right"), Edge("j", "s")))
    FlowValidator.validate(j, catalog) shouldBe Nil
  }

  "a stream needs a source and a sink" in {
    val noSink = FlowDef("f", "F", List(n("a", "t.source"), n("op", "t.op")), List(Edge("a", "op")))
    FlowValidator.validate(noSink, catalog) shouldBe List(FlowError.StreamWithoutSink(List("a", "op")))
    val noSource = FlowDef("f", "F", List(n("op", "t.op"), n("s", "t.sink")), List(Edge("op", "s")))
    FlowValidator.validate(noSource, catalog) shouldBe List(FlowError.StreamWithoutSource(List("op", "s")))
    codes(FlowDef("f", "F", List(n("a", "t.source")))) shouldBe List("stream-without-sink")
  }

  "two sinks of a stream may call their output the same: the segment answers them by node" in {
    val out = List(OutputDecl("total", path("x"), DataType.Num))
    val f = FlowDef("f", "F", List(n("a", "t.source"), n("s1", "t.sink", out), n("s2", "t.sink", out.map(_.copy(target = path("y"))))), List(Edge("a", "s1"), Edge("a", "s2")))
    FlowValidator.validate(f, catalog) shouldBe Nil
    StreamSegments.condense(f, catalog).nodes.head.outputs.map(o => o.name -> o.target.render) shouldBe List("s1.total" -> "x", "s2.total" -> "y")
    // The same name twice in one node is still a mistake.
    val twice = FlowDef("f", "F", List(n("a", "t.source"), n("s1", "t.sink", out ++ out)), List(Edge("a", "s1")))
    codes(twice) shouldBe List("duplicate-output")
  }

  "a port with several edges broadcasts: both sinks are in the same segment" in {
    val f = FlowDef("f", "F", List(n("a", "t.source"), n("s1", "t.sink"), n("s2", "t.sink")), List(Edge("a", "s1"), Edge("a", "s2")))
    FlowValidator.validate(f, catalog) shouldBe Nil
    StreamSegments.find(f, catalog).map(_.sinks) shouldBe List(List("s1", "s2"))
  }

  "stream errors say which node they are about" in {
    (FlowError.StreamWithoutSink(List("a", "op")): FlowError).asJson.hcursor.get[List[String]]("nodes") shouldBe Right(List("a", "op"))
    FlowError.StreamTokenMix(Edge("op", "after")).node shouldBe Some("after")
  }
}
