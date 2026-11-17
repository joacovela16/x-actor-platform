package xplat.flow.entity

import io.circe.Json
import io.circe.syntax._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._

/** Entities as types of flows: what the flow model sees of them through the catalog. */
class EntityTypesTest extends AnyFreeSpec with Matchers {

  import Shop._
  import DataType._

  "paths are followed through the fields of an entity" in {
    Entity("Order").at(List("lines"), catalog) shouldBe Some(ListOf(Entity("OrderLine")))
    Entity("Order").at(List("customer"), catalog) shouldBe Some(Ref("Customer"))
    Entity("Order").at(List("total"), catalog) shouldBe Some(Num)
    Entity("Order").at(List("nope"), catalog) shouldBe None
    Obj(Map("o" -> Entity("Order"))).at(List("o", "status"), catalog) shouldBe Some(Str)
    // An entity that is not known is any value; the validator reports it apart.
    Entity("Ghost").at(List("x"), catalog) shouldBe Some(AnyJson)
  }

  "an entity is only itself, or an object whose fields it has" in {
    Entity("Order").conformsTo(Entity("Order"), catalog) shouldBe true
    Entity("Order").conformsTo(Entity("Customer"), catalog) shouldBe false
    Entity("Customer").conformsTo(Obj(Map("name" -> Str)), catalog) shouldBe true
    Entity("Customer").conformsTo(Obj(Map("age" -> Num)), catalog) shouldBe false
    Obj(Map("name" -> Str)).conformsTo(Entity("Customer"), catalog) shouldBe false
    Entity("Order").conformsTo(AnyJson, catalog) shouldBe true
  }

  "a reference and the type of the key of its entity are the same thing" in {
    Ref("Customer").conformsTo(Str, catalog) shouldBe true
    Str.conformsTo(Ref("Customer"), catalog) shouldBe true
    Ref("Customer").conformsTo(Num, catalog) shouldBe false
    Ref("Customer").conformsTo(Ref("Order"), catalog) shouldBe false
    Ref("Customer").accepts(Json.fromString("C-1"), catalog) shouldBe true
    Ref("Customer").accepts(Json.fromInt(1), catalog) shouldBe false
  }

  "a value of a type that holds entities is made by them, and its problems say where" in {
    val lines = json("""[{"sku": "A", "qty": 2, "price": 3}, {"sku": "B", "qty": 0, "price": 1}]""")
    ListOf(Entity("OrderLine")).conform(lines, catalog).left.map(_.map(_.render)) shouldBe Left(List("1.qty: must be at least 1"))
    val made = ListOf(Entity("OrderLine")).conform(lines.mapArray(_.take(1)), catalog).toOption.get
    made.hcursor.downN(0).get[BigDecimal]("subtotal") shouldBe Right(BigDecimal(6))
    Obj(Map("line" -> Entity("OrderLine"))).conform(Json.obj(), catalog).left.map(_.map(_.render)) shouldBe Left(List("line: is missing"))
    // Without the catalog no entity is known.
    Entity("OrderLine").accepts(Json.obj()) shouldBe false
  }

  "entity and ref types go to JSON and back" in {
    val t: DataType = Obj(Map("o" -> Entity("Order"), "c" -> ListOf(Ref("Customer"))))
    t.asJson.as[DataType] shouldBe Right(t)
    (Entity("Order"): DataType).asJson shouldBe json("""{"type": "entity", "name": "Order"}""")
    (Ref("Customer"): DataType).asJson shouldBe json("""{"type": "ref", "entity": "Customer"}""")
    Entity("Order").render shouldBe "Order"
    Ref("Customer").render shouldBe "ref Customer"
  }

  "flows" - {
    // A node that reads the path of its configuration with the type of its configuration.
    val read = NodeDescriptor(
      kind = "test.read",
      category = "Test",
      label = "Read",
      config = ConfigSchema(List(ConfigSchema.Field("path", Str), ConfigSchema.Field("type", AnyJson))),
      reads = p => (for {
        path <- p.hcursor.get[String]("path").toOption.flatMap(StatePath.parse(_).toOption)
        tpe <- p.hcursor.get[DataType]("type").toOption
      } yield StateRead(path, tpe)).toList
    )
    val nodes = new NodeCatalog(List(read))
    def reading(path: String, tpe: DataType) = NodeDef("r", "test.read", Json.obj("path" -> path.asJson, "type" -> tpe.asJson))
    def flow(node: NodeDef, input: DataType = Entity("Order")) =
      FlowDef("f", "F", nodes = List(node), inputs = List(OutputDecl("order", StatePath(List("order")), input)))
    def codes(f: FlowDef, types: Types = catalog) = FlowValidator.validate(f, nodes, types = types).map(_.code)

    "read through the fields of the entities of their state" in {
      codes(flow(reading("order.customer", Str))) shouldBe Nil
      codes(flow(reading("order.lines", ListOf(Entity("OrderLine"))))) shouldBe Nil
      codes(flow(reading("order.total", Num))) shouldBe Nil
      codes(flow(reading("order", Entity("Order")))) shouldBe Nil
      codes(flow(reading("order.total", Str))) shouldBe List("state-type-mismatch")
      codes(flow(reading("order.nope", AnyJson))) shouldBe List("state-not-declared")
    }

    "name entities that exist" in {
      codes(flow(reading("order", AnyJson), input = Entity("Ghost"))) shouldBe List("unknown-entity")
      codes(flow(reading("order", Entity("Ghost")))) should contain("unknown-entity")
      codes(flow(reading("order.total", Num)), types = Types.empty) shouldBe List("unknown-entity")
      val error = FlowValidator.validate(flow(reading("order", AnyJson), input = Entity("Ghost")), nodes, types = catalog).head
      error.asJson.hcursor.get[String]("entity") shouldBe Right("Ghost")
      error.message shouldBe "the input 'order' of the flow uses the entity 'Ghost', which does not exist"
    }
  }
}
