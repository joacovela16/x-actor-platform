package xplat.flow.nodes

import cats.Id
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.entity.{EntityCatalog, EntityDef}
import xplat.flow.model._

class EntityNodesTest extends AnyFreeSpec with Matchers {

  import xplat.flow.entity.dsl._

  private val Line: EntityDef =
    valueObject("Line")(field("qty", num).min(1), field("price", num), computed("subtotal", num)(path("qty") * path("price")))

  private val Order: EntityDef = entity("Order", key = "id")(
    field("id", str),
    field("lines", listOf(Line)).minItems(1),
    computed("total", num)(sum("lines")(path("subtotal"))),
    field("status", str)
  ).lifecycle("status", initial = "draft")(transition("approve", from = "draft", to = "approved").guard(path("total") > 0))

  private val catalog = EntityCatalog(List(Line, Order))
  private val nodes = EntityNodes.nodes[Id](catalog)

  private def json(text: String): Json = parse(text).fold(e => throw e, identity)
  private def node(kind: String): NodeType[Id] = nodes.find(_.descriptor.kind == kind).get
  private def run(kind: String, params: String, state: String, outputs: List[OutputDecl] = Nil): NodeOutput =
    node(kind).run(NodeInput(json(params), json(state), outputs = outputs))

  private val order = """{"order": {"id": "O-1", "lines": [{"qty": 2, "price": 10}]}}"""

  "entity.validate" - {
    "goes through valid with the value as the entity makes it" in {
      val out = run(EntityNodes.Validate, """{"entity": "Order", "path": "order"}""", order)
      out.port shouldBe EntityNodes.Valid
      out.values("value").hcursor.get[BigDecimal]("total") shouldBe Right(BigDecimal(20))
      out.values("value").hcursor.get[String]("status") shouldBe Right("draft")
    }

    "goes through invalid with every problem" in {
      val out = run(EntityNodes.Validate, """{"entity": "Order", "path": "order"}""", """{"order": {"id": "O-1", "lines": []}}""")
      out.port shouldBe EntityNodes.Invalid
      out.values("errors") shouldBe json("""[{"path": "lines", "rule": "minItems", "message": "must have at least 1 item"}]""")
      EntityNodes.ErrorsType.accepts(out.values("errors")) shouldBe true
    }

    "answers only what it declared" in {
      val errorsOnly = List(OutputDecl("errors", StatePath(List("orderErrors")), EntityNodes.ErrorsType))
      run(EntityNodes.Validate, """{"entity": "Order", "path": "order"}""", order, errorsOnly).values shouldBe Map.empty
    }

    "fails for a value that is not in the state, or an entity that does not exist" in {
      run(EntityNodes.Validate, """{"entity": "Order", "path": "nope"}""", order).failed shouldBe true
      run(EntityNodes.Validate, """{"entity": "Ghost", "path": "order"}""", order).error.get should include("'Ghost'")
    }
  }

  "entity.transition" - {
    "moves the value to the target of the transition" in {
      val out = run(EntityNodes.Transition, """{"entity": "Order", "path": "order", "transition": "approve"}""", order)
      out.port shouldBe Port.Out
      out.values("value").hcursor.get[String]("status") shouldBe Right("approved")
    }

    "is rejected from a state it does not start from" in {
      val approved = """{"order": {"id": "O-1", "lines": [{"qty": 2, "price": 10}], "status": "approved"}}"""
      val out = run(EntityNodes.Transition, """{"entity": "Order", "path": "order", "transition": "approve"}""", approved)
      out.port shouldBe EntityNodes.Rejected
      out.values("errors").hcursor.downN(0).get[String]("rule") shouldBe Right("transition")
    }
  }

  "entity.build" - {
    "makes a value from the state" in {
      val params = """{"entity": "Line", "values": {"qty": "${state.q}", "price": 4}}"""
      val out = run(EntityNodes.Build, params, """{"q": 3}""")
      out.port shouldBe Port.Out
      out.values("value") shouldBe json("""{"qty": 3, "price": 4, "subtotal": 12}""")
    }

    "goes through invalid when the value breaks the entity, and fails when a reference is missing" in {
      run(EntityNodes.Build, """{"entity": "Line", "values": {"qty": 0, "price": 4}}""", "{}").port shouldBe EntityNodes.Invalid
      run(EntityNodes.Build, """{"entity": "Line", "values": {"qty": "${state.nope}", "price": 4}}""", "{}").failed shouldBe true
    }
  }

  "the descriptions" - {
    "declare the value with the type of its entity, and the problems next to it" in {
      val d = EntityNodes.validateDescriptor
      d.defaultOutputs(json("""{"entity": "Order", "path": "in.order"}""")) shouldBe List(
        OutputDecl("value", StatePath(List("in", "order")), DataType.Entity("Order")),
        OutputDecl("errors", StatePath(List("in", "orderErrors")), EntityNodes.ErrorsType)
      )
      EntityNodes.buildDescriptor.defaultOutputs(json("""{"entity": "OrderLine"}""")).head.target shouldBe StatePath(List("orderLine"))
    }

    "say which outputs each port writes" in {
      val ports = EntityNodes.transitionDescriptor.ports(Json.obj())
      ports.map(p => p.name -> p.writes) shouldBe List(Port.Out -> Some(List("value")), EntityNodes.Rejected -> Some(List("errors")))
    }

    "read what they need, a transition only a value of the entity" in {
      EntityNodes.validateDescriptor.reads(json("""{"entity": "Order", "path": "order"}""")) shouldBe List(StateRead(StatePath(List("order")), DataType.AnyJson))
      EntityNodes.transitionDescriptor.reads(json("""{"entity": "Order", "path": "order", "transition": "approve"}""")) shouldBe
        List(StateRead(StatePath(List("order")), DataType.Entity("Order")))
      EntityNodes.buildDescriptor.reads(json("""{"entity": "Line", "values": {"qty": "${state.a.b}"}}""")).map(_.path.render) shouldBe List("a.b")
    }

    "check their configuration" in {
      EntityNodes.validateDescriptor.checkParams(json("""{"entity": "", "path": "a..b"}""")).size shouldBe 2
      EntityNodes.transitionDescriptor.checkParams(json("""{"entity": "Order", "path": "o"}""")) shouldBe List("transition: the name of a transition is needed")
      EntityNodes.buildDescriptor.checkParams(json("""{"entity": "Line", "values": [1]}""")) shouldBe List("values: must be an object")
    }
  }
}
