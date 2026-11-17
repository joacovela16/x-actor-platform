package xplat.flow.model

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ExprTest extends AnyFreeSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)
  private def eval(expr: String, element: String = "{}"): Either[String, Json] = Expr.parse(json(expr)).flatMap(_.eval(json(element)))

  "values and fields" in {
    eval("5") shouldBe Right(Json.fromInt(5))
    eval("\"x\"") shouldBe Right(Json.fromString("x"))
    eval("""{"const": {"a": 1}}""") shouldBe Right(json("""{"a":1}"""))
    eval("""{"path": "order.total"}""", """{"order":{"total":7}}""") shouldBe Right(Json.fromInt(7))
    eval("""{"path": "order.total"}""") shouldBe Left("'order.total' is not in the element")
    eval("""{"path": "x", "default": 0}""") shouldBe Right(Json.fromInt(0))
    eval("""{"exists": "x"}""", """{"x":null}""") shouldBe Right(Json.True)
    eval("""{"exists": "y"}""", """{"x":null}""") shouldBe Right(Json.False)
  }

  "comparison sees 1 and 1.0 as the same and orders numbers and text" in {
    eval("""{"eq": [1, 1.0]}""") shouldBe Right(Json.True)
    eval("""{"neq": ["a", "b"]}""") shouldBe Right(Json.True)
    eval("""{"gt": [{"path": "n"}, 10]}""", """{"n": 11}""") shouldBe Right(Json.True)
    eval("""{"lte": ["a", "b"]}""") shouldBe Right(Json.True)
    eval("""{"gt": [1, "a"]}""").isLeft shouldBe true
    eval("""{"in": [2, [1, 2, 3]]}""") shouldBe Right(Json.True)
  }

  "logic" in {
    eval("""{"and": [true, {"gt": [3, 2]}]}""") shouldBe Right(Json.True)
    eval("""{"or": [false, false]}""") shouldBe Right(Json.False)
    eval("""{"not": true}""") shouldBe Right(Json.False)
    eval("""{"and": [true, 1]}""").isLeft shouldBe true
    eval("""{"if": [{"gt": [{"path": "n"}, 0]}, "pos", "neg"]}""", """{"n": -1}""") shouldBe Right(Json.fromString("neg"))
  }

  "arithmetic and text" in {
    eval("""{"add": [1, 2, 3]}""") shouldBe Right(Json.fromInt(6))
    eval("""{"mul": [{"path": "p"}, {"path": "q"}]}""", """{"p": 2.5, "q": 4}""").map(_.asNumber.flatMap(_.toBigDecimal)) shouldBe Right(Some(BigDecimal(10)))
    eval("""{"div": [1, 0]}""") shouldBe Left("division by zero")
    eval("""{"add": [1, "a"]}""").isLeft shouldBe true
    eval("""{"concat": ["a", 1, "b"]}""") shouldBe Right(Json.fromString("a1b"))
    eval("""{"len": {"path": "l"}}""", """{"l": [1,2,3]}""") shouldBe Right(Json.fromInt(3))
  }

  "an expression that is not one is reported when it is read, not when it runs" in {
    Expr.parse(json("""{"bogus": 1}""")).isLeft shouldBe true
    Expr.parse(json("""{"eq": [1]}""")).isLeft shouldBe true
    Expr.parse(json("""{"path": 5}""")).isLeft shouldBe true
    Expr.parse(json("""{"if": [1, 2]}""")).isLeft shouldBe true
    Expr.parse(json("""{"path": "a..b"}""")).isLeft shouldBe true
    Expr.parseAll(json("""{"gt": [1, 2]}""")) shouldBe Nil
  }

  "operators over the items of a list" in {
    val order = """{"lines": [{"qty": 2, "price": 10}, {"qty": 1, "price": 5}], "tags": ["a", "b"]}"""
    def n(r: Either[String, Json]) = r.map(_.asNumber.flatMap(_.toBigDecimal))
    n(eval("""{"sum": ["lines", {"mul": [{"path": "qty"}, {"path": "price"}]}]}""", order)) shouldBe Right(Some(BigDecimal(25)))
    eval("""{"count": ["lines"]}""", order) shouldBe Right(Json.fromInt(2))
    eval("""{"count": ["lines", {"gt": [{"path": "qty"}, 1]}]}""", order) shouldBe Right(Json.fromInt(1))
    eval("""{"min": ["lines", {"path": "price"}]}""", order) shouldBe Right(Json.fromInt(5))
    eval("""{"max": ["tags"]}""", order) shouldBe Right(Json.fromString("b"))
    eval("""{"all": ["lines", {"gt": [{"path": "qty"}, 0]}]}""", order) shouldBe Right(Json.True)
    eval("""{"any": ["lines", {"gt": [{"path": "qty"}, 5]}]}""", order) shouldBe Right(Json.False)
    // A list that is not there is empty.
    eval("""{"sum": ["nothing", {"path": "x"}]}""", order) shouldBe Right(Json.fromInt(0))
    eval("""{"min": ["nothing"]}""", order) shouldBe Right(Json.Null)
    eval("""{"all": ["nothing", {"path": "x"}]}""", order) shouldBe Right(Json.True)
    eval("""{"sum": ["tags"]}""", order).isLeft shouldBe true
    Expr.parse(json("""{"sum": [1]}""")).isLeft shouldBe true
    Expr.parse(json("""{"sum": ["a", 1, 2]}""")).isLeft shouldBe true
  }

  "patterns and rounding" in {
    eval("""{"matches": [{"path": "s"}, "^[0-9]+$"]}""", """{"s": "123"}""") shouldBe Right(Json.True)
    eval("""{"matches": [{"path": "s"}, "^[0-9]+$"]}""", """{"s": "12a"}""") shouldBe Right(Json.False)
    Expr.parse(json("""{"matches": ["a", "(oops"]}""")).isLeft shouldBe true
    Expr.parse(json("""{"matches": ["a", {"path": "p"}]}""")).isLeft shouldBe true
    eval("""{"round": [2.345, 2]}""") shouldBe Right(Json.fromBigDecimal(BigDecimal("2.35")))
    eval("""{"round": [2.5, 0]}""") shouldBe Right(Json.fromBigDecimal(BigDecimal(3)))
    eval("""{"round": [1, -1]}""").isLeft shouldBe true
  }

  "an expression is written back as the JSON it was read from" in {
    List(
      """{"path": "a", "default": 0}""",
      """{"const": {"a": 1}}""",
      """{"exists": "a.b"}""",
      """{"not": {"path": "a"}}""",
      """{"if": [true, 1, 2]}""",
      """{"and": [{"gt": [{"path": "a"}, 1]}, {"in": [{"path": "b"}, [1, 2]]}]}""",
      """{"sum": ["lines", {"path": "subtotal"}]}""",
      """{"count": ["lines"]}"""
    ).foreach { text =>
      Expr.parse(json(text)).map(_.toJson) shouldBe Right(json(text))
    }
  }

  "what an expression reads is checked against a type, inside lists by the type of their items" in {
    val tpe = DataType.Obj(Map("a" -> DataType.Num, "lines" -> DataType.ListOf(DataType.Obj(Map("qty" -> DataType.Num)))))
    def check(text: String) = Expr.parse(json(text)).toOption.get.check(tpe)
    check("""{"add": [{"path": "a"}, {"sum": ["lines", {"path": "qty"}]}]}""") shouldBe Nil
    check("""{"path": "b"}""") shouldBe List("'b' is not a field")
    check("""{"sum": ["lines", {"path": "price"}]}""") shouldBe List("'price' is not a field")
    check("""{"sum": ["a", {"path": "x"}]}""") shouldBe List("'a' is num, not a list")
    Expr.parse(json("""{"add": [{"path": "a"}, {"sum": ["lines", {"path": "x"}]}]}""")).toOption.get.paths.map(_.render) shouldBe List("a", "lines")
  }
}
