package xplat.flow.entity

import io.circe.syntax._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EntityDefTest extends AnyFreeSpec with Matchers {

  import Shop._

  "the DSL writes the same entity as the JSON of the editor" in {
    OrderJson.as[EntityDef] shouldBe Right(Order)
  }

  "an entity goes to JSON and back unchanged" in {
    List(Customer, OrderLine, Order).foreach(e => e.asJson.as[EntityDef] shouldBe Right(e))
  }

  "the JSON leaves out what says nothing" in {
    val line = OrderLine.asJson
    line.hcursor.get[String]("kind") shouldBe Right("value")
    line.hcursor.downField("key").succeeded shouldBe false
    line.hcursor.downField("rules").succeeded shouldBe false
    line.hcursor.downField("fields").downArray.downField("required").succeeded shouldBe false
  }

  "an expression that is not valid is a decoding failure that says where" in {
    val bad = json("""{"name": "X", "key": "id", "fields": [{"name": "id", "type": {"type": "str"}, "computed": {"sum": 3}}]}""")
    val failure = bad.as[EntityDef].left.toOption.get
    failure.getMessage should include("'sum' takes")
  }
}
