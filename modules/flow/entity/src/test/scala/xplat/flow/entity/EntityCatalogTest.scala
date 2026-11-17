package xplat.flow.entity

import io.circe.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model.DataType.Problem

class EntityCatalogTest extends AnyFreeSpec with Matchers {

  import Shop._

  private val order = json("""{"id": "O-1", "customer": "C-1", "lines": [{"sku": "A", "qty": 2, "price": 10}, {"sku": "B", "qty": 1, "price": 5.5}]}""")

  private def problems(entity: String, value: Json): List[(String, String)] =
    catalog.check(entity, value).problems.map(p => p.path.mkString(".") -> p.rule)

  private def numberAt(v: Json, path: String*): BigDecimal =
    path.foldLeft(v.hcursor: io.circe.ACursor)((c, p) => if (p.forall(_.isDigit)) c.downN(p.toInt) else c.downField(p)).as[BigDecimal].toOption.get

  "a valid value gets its defaults, its initial state and its computed fields, nested ones first" in {
    val checked = catalog.check("Order", order)
    checked.problems shouldBe Nil
    numberAt(checked.value, "lines", "0", "subtotal") shouldBe BigDecimal(20)
    numberAt(checked.value, "lines", "1", "subtotal") shouldBe BigDecimal(5.5)
    numberAt(checked.value, "total") shouldBe BigDecimal(25.5)
    numberAt(checked.value, "items") shouldBe BigDecimal(2)
    checked.value.hcursor.get[String]("status") shouldBe Right("draft")
  }

  "a computed field that the value brings is replaced, and fields the entity does not know are kept" in {
    val brought = order.mapObject(_.add("total", Json.fromInt(1000)).add("channel", Json.fromString("web")))
    val checked = catalog.check("Order", brought)
    numberAt(checked.value, "total") shouldBe BigDecimal(25.5)
    checked.value.hcursor.get[String]("channel") shouldBe Right("web")
  }

  "every problem comes at once, with the path where it is" in {
    val bad = json("""{"id": "O-1", "customer": 7, "lines": [{"sku": "A", "qty": 0, "price": 10}, {"qty": 1, "price": -1}]}""")
    problems("Order", bad) should contain theSameElementsAs List(
      "customer" -> "type",
      "lines.0.qty" -> "min",
      "lines.1.sku" -> "required",
      "lines.1.price" -> "min"
    )
    problems("Order", json("""{"id": "O-1", "customer": "C-1", "lines": []}""")) shouldBe List("lines" -> "minItems")
    problems("Order", json("""{"customer": "C-1", "lines": null}""")) shouldBe List("id" -> "required", "lines" -> "required")
  }

  "a value that is not an object, or of an entity that does not exist" in {
    problems("Order", Json.fromInt(3)) shouldBe List("" -> "type")
    problems("Nope", Json.obj()) shouldBe List("" -> "unknown-entity")
  }

  "constraints of text, and a rule that applies only when its condition holds" in {
    problems("Customer", json("""{"id": "C-1", "name": "Ana"}""")) shouldBe Nil
    catalog.check("Customer", json("""{"id": "C-1", "name": "Ana"}""")).value.hcursor.get[String]("kind") shouldBe Right("person")
    problems("Customer", json("""{"id": "X1", "name": "", "kind": "robot"}""")) shouldBe List("id" -> "pattern", "name" -> "minLength", "kind" -> "oneOf")
    val company = catalog.check("Customer", json("""{"id": "C-2", "name": "ACME", "kind": "company"}"""))
    company.problems shouldBe List(Problem(Nil, "company-tax-id", "A company needs a tax id"))
    problems("Customer", json("""{"id": "C-2", "name": "ACME", "kind": "company", "taxId": "30-1"}""")) shouldBe Nil
  }

  "a state that is not one of the states" in {
    problems("Order", order.mapObject(_.add("status", Json.fromString("lost")))) shouldBe List("status" -> "state")
  }

  "transitions" - {
    def move(value: Json, name: String) = catalog.transition("Order", value, name)
    def status(r: Either[List[Problem], Json]) = r.map(_.hcursor.get[String]("status").toOption.get)
    def rules(r: Either[List[Problem], Json]) = r.left.map(_.map(_.rule))

    "go from a state the transition starts from, to its target" in {
      val approved = move(order, "approve")
      status(approved) shouldBe Right("approved")
      status(move(order, "cancel")) shouldBe Right("cancelled")
      rules(move(order, "ship")) shouldBe Left(List("transition"))
    }

    "need their guard to hold, and say its message" in {
      val free = json("""{"id": "O-2", "customer": "C-1", "lines": [{"sku": "A", "qty": 1, "price": 0}]}""")
      move(free, "approve") shouldBe Left(List(Problem(Nil, "approve", "The total must be positive")))
    }

    "leave a value that keeps the rules of its new state" in {
      val approved = move(order, "approve").toOption.get
      rules(move(approved, "ship")) shouldBe Left(List("shipped-tracking"))
      status(move(approved.mapObject(_.add("trackingId", Json.fromString("T-9"))), "ship")) shouldBe Right("shipped")
    }

    "are refused for a value that is not valid, or that do not exist" in {
      rules(move(json("""{"id": "O-1"}"""), "approve")) shouldBe Left(List("required", "required"))
      rules(move(order, "teleport")) shouldBe Left(List("transition"))
      rules(catalog.transition("Customer", Json.obj(), "x")) shouldBe Left(List("transition"))
    }
  }

  "a computed field that cannot be computed says why" in {
    import dsl._
    val ratio = valueObject("Ratio")(field("a", num), field("b", num), computed("r", num)(path("a") / path("b")))
    val c = EntityCatalog(List(ratio))
    c.check("Ratio", json("""{"a": 1, "b": 0}""")).problems.map(p => p.path -> p.rule) shouldBe List(List("r") -> "computed")
    // When another problem explains it, the computed field says nothing.
    c.check("Ratio", json("""{"a": 1}""")).problems.map(_.rule) shouldBe List("required")
  }

  "who uses an entity" in {
    catalog.usersOf("Customer") shouldBe List("Order")
    catalog.usersOf("OrderLine") shouldBe List("Order")
    catalog.usersOf("Order") shouldBe Nil
  }
}
