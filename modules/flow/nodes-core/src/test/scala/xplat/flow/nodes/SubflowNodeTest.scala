package xplat.flow.nodes

import cats.Id
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._

class SubflowNodeTest extends AnyFreeSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)
  private def path(text: String): StatePath = StatePath.parse(text).toOption.get
  private def decl(name: String, target: String, tpe: DataType) = OutputDecl(name, path(target), tpe)

  // A flow that takes an order total and exposes whether it is approved.
  private def approval(revision: Int, approved: DataType = DataType.Bool) = FlowDef(
    "approval", "Approval",
    revision = revision,
    inputs = List(decl("total", "total", DataType.Num)),
    outputs = List(decl("approved", "result.approved", approved)),
    nodes = List(NodeDef("n", IfNode.Kind, json("""{"path":"x"}"""))).take(0)
  )

  private def call(params: String, outputs: List[OutputDecl] = Nil): NodeDef =
    NodeDef("call", SubflowNode.Kind, json(params), outputs)

  private def caller(node: NodeDef, inputs: List[OutputDecl] = List(decl("order", "order", DataType.Obj(Map("total" -> DataType.Num))))) =
    FlowDef("main", "Main", List(node), inputs = inputs)

  private val ok = """{"flowId":"approval","inputs":{"total":"order.total"}}"""
  private val takes = List(decl("approved", "approved", DataType.Bool))

  private def codes(flow: FlowDef, flows: FlowDef*): List[String] =
    FlowValidator.validate(flow, CoreNodes.catalog, FlowResolver.of(flows)).map(_.code)

  "a call to a flow that exists, with its inputs filled and its outputs taken, is valid" in {
    FlowValidator.validate(caller(call(ok, takes)), CoreNodes.catalog, FlowResolver.of(List(approval(1)))) shouldBe Nil
  }

  "flows expose outputs and have a revision in their JSON" in {
    val flow = approval(3)
    json(flow.asJson.noSpaces).as[FlowDef] shouldBe Right(flow)
    json("""{"id":"f","name":"F","nodes":[]}""").as[FlowDef].map(_.revision) shouldBe Right(1)
  }

  "a flow that does not exist is reported" in {
    codes(caller(call(ok, takes))) shouldBe List("unknown-flow")
    codes(caller(call("""{"flowId":"approval","revision":7,"inputs":{"total":"order.total"}}""")), approval(1)) shouldBe List("unknown-flow")
  }

  "the latest revision is used unless one is named" in {
    // Only revision 2 exposes the output with the type the caller expects.
    val old = approval(1, DataType.Str)
    val latest = approval(2)
    codes(caller(call(ok, takes)), old, latest) shouldBe Nil
    codes(caller(call("""{"flowId":"approval","revision":1,"inputs":{"total":"order.total"}}""", takes)), old, latest) shouldBe List("flow-output-mismatch")
  }

  "inputs must exist, all be filled, and be of the type the called flow expects" in {
    codes(caller(call("""{"flowId":"approval","inputs":{"total":"order.total","extra":"order.total"}}""")), approval(1)) shouldBe List("unknown-flow-input")
    codes(caller(call("""{"flowId":"approval"}""")), approval(1)) shouldBe List("missing-flow-input")
    codes(caller(call(ok), inputs = Nil), approval(1)) shouldBe List("state-not-declared")
    val text = List(decl("order", "order", DataType.Obj(Map("total" -> DataType.Str))))
    FlowValidator.validate(caller(call(ok), text), CoreNodes.catalog, FlowResolver.of(List(approval(1)))) shouldBe
      List(FlowError.StateTypeMismatch("call", path("order.total"), DataType.Num, DataType.Str))
  }

  "outputs taken must be exposed with the type expected" in {
    codes(caller(call(ok, List(decl("nope", "x", DataType.Bool)))), approval(1)) shouldBe List("unknown-flow-output")
    codes(caller(call(ok, List(decl("approved", "x", DataType.Num)))), approval(1)) shouldBe List("flow-output-mismatch")
  }

  "without waiting there are no outputs to take" in {
    val params = """{"flowId":"approval","wait":false,"inputs":{"total":"order.total"}}"""
    codes(caller(call(params)), approval(1)) shouldBe Nil
    codes(caller(call(params, takes)), approval(1)) shouldBe List("outputs-without-waiting")
  }

  "flows that call each other in a circle are reported, also a flow that calls itself" in {
    def calling(id: String, target: String) =
      FlowDef(id, id, List(NodeDef("c", SubflowNode.Kind, json(s"""{"flowId":"$target"}"""))))
    val a = calling("a", "b")
    val b = calling("b", "a")
    FlowValidator.validate(a, CoreNodes.catalog, FlowResolver.of(List(a, b))) shouldBe List(FlowError.FlowCycle(List("a", "b", "a")))
    val self = calling("s", "s")
    FlowValidator.validate(self, CoreNodes.catalog, FlowResolver.of(List(self))) shouldBe List(FlowError.FlowCycle(List("s", "s")))
    // A flow that calls one that is in a circle without it is not itself in a circle, and the search ends.
    val c = calling("c", "a")
    FlowValidator.validate(c, CoreNodes.catalog, FlowResolver.of(List(a, b, c))) shouldBe Nil
  }

  "configuration is validated" in {
    codes(caller(call("""{"inputs":{}}"""))) shouldBe List("invalid-params")
    codes(caller(call("""{"flowId":"x","inputs":{"a":"b..c"}}"""))) shouldBe List("invalid-params")
  }

  // A runner that records what it was asked and answers with what it is told to.
  private final class Fake(answer: FlowRunner.Result) extends FlowRunner[Id] {
    var asked: Option[(String, Option[Int], Map[String, Json], Boolean)] = None
    var caller: Option[FlowRunner.Caller] = None
    def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, who: FlowRunner.Caller): FlowRunner.Result = {
      caller = Some(who)
      asked = Some((flowId, revision, inputs, wait))
      answer
    }
  }

  private def run(node: SubflowNode[Id], params: String, state: String) = node.run(NodeInput(json(params), json(state)))

  "it fills the inputs from the state and takes the outputs" in {
    val runner = new Fake(FlowRunner.Finished(Map("approved" -> Json.True)))
    val out = run(new SubflowNode[Id](runner), """{"flowId":"approval","revision":2,"inputs":{"total":"order.total"}}""", """{"order":{"total":10}}""")
    out shouldBe NodeOutput.ok(Map("approved" -> Json.True))
    runner.asked shouldBe Some(("approval", Some(2), Map("total" -> Json.fromInt(10)), true))
  }

  "it says who calls and records the instance that ran the flow" in {
    val runner = new Fake(FlowRunner.Finished(Map.empty, Some("child-1")))
    val out = new SubflowNode[Id](runner).run(NodeInput(json("""{"flowId":"approval"}"""), json("{}"), instanceId = "i-1", nodeId = "call"))
    runner.caller shouldBe Some(FlowRunner.Caller("i-1", "call"))
    out.info shouldBe Map(SubflowNode.ChildInstance -> Json.fromString("child-1"))
  }

  "it passes on that it must not wait" in {
    val runner = new Fake(FlowRunner.Finished(Map.empty))
    run(new SubflowNode[Id](runner), """{"flowId":"approval","wait":false}""", "{}").failed shouldBe false
    runner.asked.map(_._4) shouldBe Some(false)
  }

  "a flow that fails, or an input that is not in the state, goes through the error port" in {
    val failing = run(new SubflowNode[Id](new Fake(FlowRunner.Failed("boom"))), """{"flowId":"approval"}""", "{}")
    failing.port shouldBe Port.Error
    failing.error.get should include("boom")
    val runner = new Fake(FlowRunner.Finished(Map.empty))
    val missing = run(new SubflowNode[Id](runner), """{"flowId":"approval","inputs":{"total":"order.total"}}""", "{}")
    missing.port shouldBe Port.Error
    runner.asked shouldBe None
    run(new SubflowNode[Id](runner), "{}", "{}").port shouldBe Port.Error
  }

  "the registry of the core nodes has both and matches the catalog" in {
    val registry = CoreNodes.registry[Id](new Fake(FlowRunner.Finished(Map.empty)))
    registry.catalog.all.map(_.kind) shouldBe CoreNodes.catalog.all.map(_.kind)
    registry.get(SubflowNode.Kind) should not be empty
  }
}
