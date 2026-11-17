package xplat.flow.nodes

import cats.Id
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._

class IfNodeTest extends AnyFreeSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)

  private val node = new IfNode[Id]

  private def run(params: String, state: String = "{}", memory: String = "{}"): NodeOutput =
    node.run(NodeInput(json(params), json(state), json(memory)))

  "goes through then when the variable is true, else when it is false" in {
    run("""{"path":"approved"}""", """{"approved":true}""").port shouldBe IfNode.Then
    run("""{"path":"approved"}""", """{"approved":false}""").port shouldBe IfNode.Else
  }

  "reads nested paths" in {
    run("""{"path":"order.ok"}""", """{"order":{"ok":true}}""").port shouldBe IfNode.Then
  }

  "negate swaps the ports" in {
    run("""{"path":"a","negate":true}""", """{"a":true}""").port shouldBe IfNode.Else
    run("""{"path":"a","negate":true}""", """{"a":false}""").port shouldBe IfNode.Then
  }

  "reads the memory of the node when asked to" in {
    run("""{"path":"a","source":"memory"}""", state = """{"a":false}""", memory = """{"a":true}""").port shouldBe IfNode.Then
    // The state is not looked at, so a variable that is only there fails.
    run("""{"path":"a","source":"memory"}""", state = """{"a":true}""").failed shouldBe true
  }

  "a variable that is missing fails through the error port instead of taking else" in {
    val out = run("""{"path":"approved"}""")
    out.failed shouldBe true
    out.port shouldBe Port.Error
    out.error.get should include("not in the state")
  }

  "a variable that is not a boolean fails through the error port" in {
    val out = run("""{"path":"approved"}""", """{"approved":"yes"}""")
    out.port shouldBe Port.Error
    out.error.get should include("not a boolean")
  }

  "bad configuration fails instead of throwing" in {
    run("""{"source":"state"}""").port shouldBe Port.Error
    run("""{"path":"a..b"}""").port shouldBe Port.Error
  }

  "it writes nothing in the state" in {
    run("""{"path":"a"}""", """{"a":true}""").values shouldBe empty
  }

  "its description tells the validator what it needs" in {
    val d = IfNode.descriptor
    d.portNames(json("""{"path":"a"}""")) shouldBe List("then", "else", Port.Error)
    d.sideEffects shouldBe SideEffects.Pure
    d.reads(json("""{"path":"order.ok"}""")) shouldBe List(StateRead(StatePath(List("order", "ok")), DataType.Bool))
    d.reads(json("""{"path":"a","source":"memory"}""")) shouldBe Nil
  }

  "its configuration is validated" in {
    val d = IfNode.descriptor
    d.config.validate(json("""{"path":"a"}""")) shouldBe Nil
    d.config.validate(json("""{"path":"a","source":"memory","negate":true}""")) shouldBe Nil
    d.config.validate(json("{}")) shouldBe List("missing field 'path'")
    d.config.validate(json("""{"path":"a","source":"disk"}""")).size shouldBe 1
    d.checkParams(json("""{"path":"a..b"}""")).size shouldBe 1
  }

  "a flow with an if is validated against the registry of the core nodes" in {
    val catalog = CoreNodes.catalog
    val ok = FlowDef(
      "f", "F",
      inputs = List(OutputDecl("approved", StatePath(List("approved")), DataType.Bool)),
      nodes = List(NodeDef("check", IfNode.Kind, json("""{"path":"approved"}""")), NodeDef("yes", IfNode.Kind, json("""{"path":"approved"}"""))),
      edges = List(Edge("check", "yes", IfNode.Then), Edge("check", "yes", IfNode.Else))
    )
    FlowValidator.validate(ok, catalog) shouldBe Nil

    val bad = ok.copy(inputs = Nil, edges = List(Edge("check", "yes", "maybe")))
    FlowValidator.validate(bad, catalog).map(_.code).sorted shouldBe List("state-not-declared", "state-not-declared", "unknown-port")
  }

  "the registry does not take the same kind twice" in {
    an[IllegalArgumentException] should be thrownBy NodeRegistry[Id](new IfNode[Id], new IfNode[Id])
  }
}
