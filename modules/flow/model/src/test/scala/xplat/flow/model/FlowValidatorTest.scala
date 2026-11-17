package xplat.flow.model

import io.circe.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class FlowValidatorTest extends AnyFreeSpec with Matchers {

  private def path(text: String): StatePath = StatePath.parse(text).toOption.get

  // A node that writes what its configuration says, and a node that branches on a boolean it reads.
  private val produce = NodeDescriptor(
    kind = "test.produce",
    category = "Test",
    label = "Produce",
    config = ConfigSchema(List(ConfigSchema.Field("n", DataType.Num, required = false)))
  )

  private val branch = NodeDescriptor(
    kind = "test.branch",
    category = "Test",
    label = "Branch",
    config = ConfigSchema(List(ConfigSchema.Field("path", DataType.Str), ConfigSchema.Field("expect", DataType.Str, required = false))),
    ports = _ => List(Port("then"), Port("else")),
    reads = p => p.hcursor.get[String]("path").toOption.flatMap(StatePath.parse(_).toOption).map(StateRead(_, DataType.Bool)).toList,
    checkParams = p => if (p.hcursor.get[String]("expect").contains("bad")) List("expect is bad") else Nil
  )

  private val catalog = new NodeCatalog(List(produce, branch))

  private def writes(target: String, tpe: DataType): List[OutputDecl] = List(OutputDecl("v", path(target), tpe))
  private def reading(target: String): Json = Json.obj("path" -> Json.fromString(target))

  private def codes(flow: FlowDef): List[String] = FlowValidator.validate(flow, catalog).map(_.code)

  "a correct flow has no errors" in {
    val flow = FlowDef(
      "f", "F",
      nodes = List(
        NodeDef("a", "test.produce", outputs = writes("approved", DataType.Bool)),
        NodeDef("b", "test.branch", reading("approved")),
        NodeDef("c", "test.produce"),
        NodeDef("d", "test.produce")
      ),
      edges = List(Edge("a", "b"), Edge("b", "c", "then"), Edge("b", "d", "else"))
    )
    FlowValidator.validate(flow, catalog) shouldBe Nil
  }

  "all the problems come at once" in {
    val flow = FlowDef(
      "f", "F",
      version = 9,
      nodes = List(
        NodeDef("a", "nope"),
        NodeDef("a", "test.produce", Json.obj("n" -> Json.fromString("x"))),
        NodeDef("b", "test.branch", reading("approved").mapObject(_.add("expect", Json.fromString("bad"))))
      ),
      edges = List(Edge("a", "ghost"), Edge("b", "a", "maybe"))
    )
    codes(flow).sorted shouldBe List("duplicate-node", "invalid-params", "invalid-params", "state-not-declared", "unknown-kind", "unknown-node", "unknown-port", "unsupported-version")
  }

  "a node cannot declare the same output twice" in {
    val flow = FlowDef("f", "F", List(NodeDef("a", "test.produce", outputs = writes("x", DataType.Num) ++ writes("y", DataType.Num))))
    codes(flow) shouldBe List("duplicate-output")
  }

  "cycles are found, also a node pointing at itself" in {
    val three = FlowDef("f", "F", List(NodeDef("a", "test.produce"), NodeDef("b", "test.produce"), NodeDef("c", "test.produce"), NodeDef("d", "test.produce")),
      List(Edge("a", "b"), Edge("b", "c"), Edge("c", "b"), Edge("c", "d")))
    FlowValidator.validate(three, catalog) shouldBe List(FlowError.Cycle(List("b", "c", "d")))
    val self = FlowDef("f", "F", List(NodeDef("a", "test.produce")), List(Edge("a", "a")))
    codes(self) shouldBe List("cycle")
  }

  "what a node reads must be declared before it" in {
    val undeclared = FlowDef("f", "F", List(NodeDef("b", "test.branch", reading("approved"))))
    codes(undeclared) shouldBe List("state-not-declared")

    // Declared by a node that comes after it, or by an unrelated branch, does not count.
    val after = FlowDef("f", "F",
      List(NodeDef("b", "test.branch", reading("approved")), NodeDef("a", "test.produce", outputs = writes("approved", DataType.Bool))),
      List(Edge("b", "a", "then")))
    codes(after) shouldBe List("state-not-declared")
    val sibling = FlowDef("f", "F",
      List(NodeDef("r", "test.produce"), NodeDef("a", "test.produce", outputs = writes("approved", DataType.Bool)), NodeDef("b", "test.branch", reading("approved"))),
      List(Edge("r", "a"), Edge("r", "b")))
    codes(sibling) shouldBe List("state-not-declared")
  }

  "the inputs of the flow count as declared" in {
    val flow = FlowDef("f", "F", List(NodeDef("b", "test.branch", reading("approved"))), inputs = writes("approved", DataType.Bool))
    FlowValidator.validate(flow, catalog) shouldBe Nil
  }

  "the type of what is read must fit" in {
    val flow = FlowDef("f", "F",
      List(NodeDef("a", "test.produce", outputs = writes("approved", DataType.Str)), NodeDef("b", "test.branch", reading("approved"))),
      List(Edge("a", "b")))
    FlowValidator.validate(flow, catalog) shouldBe List(FlowError.StateTypeMismatch("b", path("approved"), DataType.Bool, DataType.Str))
  }

  "a field inside an object declared as a whole is found" in {
    val order = DataType.Obj(Map("approved" -> DataType.Bool, "total" -> DataType.Num))
    val flow = FlowDef("f", "F",
      List(NodeDef("a", "test.produce", outputs = writes("order", order)), NodeDef("b", "test.branch", reading("order.approved")), NodeDef("c", "test.branch", reading("order.total"))),
      List(Edge("a", "b"), Edge("a", "c")))
    FlowValidator.validate(flow, catalog) shouldBe List(FlowError.StateTypeMismatch("c", path("order.total"), DataType.Bool, DataType.Num))
  }

  "branches that declare the same variable in different ways: one that fits is enough" in {
    // then/else both write the flag, as usual; the join reads it.
    val flow = FlowDef("f", "F",
      List(
        NodeDef("i", "test.branch", reading("start")),
        NodeDef("t", "test.produce", outputs = writes("flag", DataType.Bool)),
        NodeDef("e", "test.produce", outputs = writes("flag", DataType.Bool)),
        NodeDef("j", "test.branch", reading("flag"))
      ),
      List(Edge("i", "t", "then"), Edge("i", "e", "else"), Edge("t", "j"), Edge("e", "j")),
      inputs = writes("start", DataType.Bool))
    FlowValidator.validate(flow, catalog) shouldBe Nil
  }

  "the error port exists in every node" in {
    val flow = FlowDef("f", "F", List(NodeDef("a", "test.produce"), NodeDef("b", "test.produce")), List(Edge("a", "b", Port.Error)))
    FlowValidator.validate(flow, catalog) shouldBe Nil
  }

  "a catalog does not take the same kind twice" in {
    an[IllegalArgumentException] should be thrownBy new NodeCatalog(List(produce, produce))
  }
}
