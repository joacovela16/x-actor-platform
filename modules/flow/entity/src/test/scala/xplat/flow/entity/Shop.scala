package xplat.flow.entity

import io.circe.Json
import io.circe.parser.parse

/** The entities of a small shop, written with the DSL, and the JSON the editor would write for the order. */
object Shop {

  import dsl._

  val Customer: EntityDef = entity("Customer", key = "id")(
    field("id", str).pattern("^C-[0-9]+$"),
    field("name", str).minLength(1),
    field("kind", str).oneOf("person", "company").default("person"),
    field("taxId", str).optional
  ).ruleWhen("company-tax-id", "A company needs a tax id")(path("kind") === "company")(exists("taxId"))

  val OrderLine: EntityDef = valueObject("OrderLine")(
    field("sku", str),
    field("qty", num).min(1),
    field("price", num).min(0),
    computed("subtotal", num)(path("qty") * path("price"))
  )

  val Order: EntityDef = entity("Order", key = "id")(
    field("id", str),
    field("customer", ref(Customer)),
    field("lines", listOf(OrderLine)).minItems(1),
    computed("total", num)(sum("lines")(path("subtotal"))),
    computed("items", num)(count("lines")),
    field("status", str),
    field("trackingId", str).optional
  ).ruleWhen("shipped-tracking", "A shipped order needs a tracking id")(path("status") === "shipped")(exists("trackingId"))
    .lifecycle("status", initial = "draft")(
      transition("approve", from = "draft", to = "approved").guard(path("total") > 0).message("The total must be positive"),
      transition("ship", from = "approved", to = "shipped"),
      transition("cancel", from = List("draft", "approved"), to = "cancelled")
    )

  val catalog: EntityCatalog = EntityCatalog(List(Customer, OrderLine, Order))

  def json(text: String): Json = parse(text).fold(e => throw e, identity)

  val OrderJson: Json = json(
    """{
      |  "name": "Order", "kind": "root", "key": "id",
      |  "fields": [
      |    {"name": "id", "type": {"type": "str"}},
      |    {"name": "customer", "type": {"type": "ref", "entity": "Customer"}},
      |    {"name": "lines", "type": {"type": "list", "element": {"type": "entity", "name": "OrderLine"}}, "constraints": {"minItems": 1}},
      |    {"name": "total", "type": {"type": "num"}, "computed": {"sum": ["lines", {"path": "subtotal"}]}},
      |    {"name": "items", "type": {"type": "num"}, "computed": {"count": ["lines"]}},
      |    {"name": "status", "type": {"type": "str"}},
      |    {"name": "trackingId", "type": {"type": "str"}, "required": false}
      |  ],
      |  "rules": [
      |    {"name": "shipped-tracking", "when": {"eq": [{"path": "status"}, "shipped"]}, "check": {"exists": "trackingId"},
      |     "message": "A shipped order needs a tracking id"}
      |  ],
      |  "states": {
      |    "field": "status", "initial": "draft", "states": ["draft", "approved", "shipped", "cancelled"],
      |    "transitions": [
      |      {"name": "approve", "from": ["draft"], "to": "approved", "guard": {"gt": [{"path": "total"}, 0]}, "message": "The total must be positive"},
      |      {"name": "ship", "from": "approved", "to": "shipped"},
      |      {"name": "cancel", "from": ["draft", "approved"], "to": "cancelled"}
      |    ]
      |  }
      |}""".stripMargin
  )
}
