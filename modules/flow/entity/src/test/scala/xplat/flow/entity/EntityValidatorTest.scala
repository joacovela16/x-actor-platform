package xplat.flow.entity

import io.circe.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EntityValidatorTest extends AnyFreeSpec with Matchers {

  import Shop.catalog
  import dsl._

  private def codes(e: EntityDef, among: EntityCatalog = catalog): List[String] = EntityValidator.validate(e, among).map(_.code)

  "the entities of the shop are valid" in {
    List(Shop.Customer, Shop.OrderLine, Shop.Order).foreach(e => EntityValidator.validate(e, catalog) shouldBe Nil)
  }

  "names" in {
    codes(entity("bad name", key = "id")(field("id", str))) shouldBe List("invalid-name")
    codes(entity("X", key = "id")(field("id", str), field("id", num), field("a b", str))) should contain allOf ("duplicate-field", "invalid-field-name")
    codes(valueObject("Empty")()) shouldBe List("no-fields")
  }

  "keys" in {
    codes(EntityDef("X", List(EntityDef.Field("id", DataTypes.str)))) shouldBe List("missing-key")
    codes(entity("X", key = "nope")(field("id", str))) shouldBe List("unknown-key")
    codes(entity("X", key = "id")(field("id", bool))) shouldBe List("invalid-key")
    codes(entity("X", key = "id")(field("id", str).optional)) shouldBe List("invalid-key")
    codes(valueObject("X")(field("id", str)).copy(key = Some("id"))) shouldBe List("invalid-key")
  }

  "types name entities that exist, and references point to roots" in {
    codes(valueObject("X")(field("a", embed("Ghost")))) shouldBe List("unknown-entity")
    codes(valueObject("X")(field("a", listOf(ref("Ghost"))))) shouldBe List("unknown-entity")
    codes(valueObject("X")(field("line", ref(Shop.OrderLine)))) shouldBe List("ref-to-value")
  }

  "a value cannot hold itself forever, but it can in a list or when the field is optional" in {
    codes(valueObject("Node")(field("child", embed("Node")))) shouldBe List("embedding-cycle")
    codes(valueObject("Node")(field("child", embed("Node")).optional)) shouldBe Nil
    codes(valueObject("Node")(field("children", listOf(embed("Node"))))) shouldBe Nil
    val a = valueObject("A")(field("b", embed("B")))
    val b = valueObject("B")(field("a", embed("A")))
    codes(a, EntityCatalog(List(b))) shouldBe List("embedding-cycle")
  }

  "constraints fit the type of their field" in {
    codes(valueObject("X")(field("n", num).pattern("x"))) shouldBe List("invalid-constraint")
    codes(valueObject("X")(field("s", str).pattern("(unclosed"))) shouldBe List("invalid-constraint")
    codes(valueObject("X")(field("n", num).min(5).max(1))) shouldBe List("invalid-constraint")
    codes(valueObject("X")(field("l", listOf(num)).minItems(-1))) shouldBe List("invalid-constraint")
    codes(valueObject("X")(field("s", str).oneOf(1, 2))) shouldBe List("invalid-constraint", "invalid-constraint")
  }

  "defaults fit the type, and computed fields have none" in {
    codes(valueObject("X")(field("n", num).default("x"))) shouldBe List("invalid-default")
    codes(valueObject("X")(field("a", num), computed("b", num)(path("a")).default(1))) shouldBe List("invalid-default")
  }

  "expressions read fields that exist, also inside the items of a list" in {
    codes(valueObject("X")(field("a", num), computed("b", num)(path("nope") + 1))) shouldBe List("invalid-expression")
    codes(valueObject("X")(field("a", num), computed("b", num)(sum("a")(path("x"))))) shouldBe List("invalid-expression")
    codes(entity("X", key = "id")(field("id", str), field("lines", listOf(Shop.OrderLine)), computed("t", num)(sum("lines")(path("nope"))))) shouldBe
      List("invalid-expression")
    codes(valueObject("X")(field("a", num)).rule("r", "m")(path("b") > 1)) shouldBe List("invalid-expression")
  }

  "computed fields do not read each other in a circle" in {
    codes(valueObject("X")(computed("a", num)(path("b") + 1), computed("b", num)(path("a") + 1))) shouldBe List("computed-cycle")
  }

  "rules have distinct names" in {
    codes(valueObject("X")(field("a", num)).rule("r", "m")(path("a") > 1).rule("r", "m")(path("a") < 9)) shouldBe List("duplicate-rule")
  }

  "states" in {
    val base = valueObject("X")(field("status", str), field("n", num))
    codes(base.lifecycle("ghost", initial = "a")(transition("go", from = "a", to = "b"))) shouldBe List("invalid-states")
    codes(base.lifecycle("n", initial = "a")(transition("go", from = "a", to = "b"))) shouldBe List("invalid-states")
    codes(base.lifecycle("status", initial = "z", all = List("a", "b"))(transition("go", from = "a", to = "c"))) shouldBe List("invalid-states", "invalid-states")
    codes(base.lifecycle("status", initial = "a")(transition("go", from = "a", to = "b"), transition("go", from = "b", to = "a"))) shouldBe List("invalid-states")
    codes(base.lifecycle("status", initial = "a")(transition("go", from = "a", to = "b").guard(path("ghost") > 1))) shouldBe List("invalid-states")
  }

  "an old version of the format" in {
    codes(Shop.OrderLine.copy(version = 9)) shouldBe List("unsupported-version")
  }

  private object DataTypes {
    val str = xplat.flow.model.DataType.Str
  }

  locally(Json.Null)
}
