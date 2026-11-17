package xplat.flow.nodes

import cats.Id
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._

class RespondNodeTest extends AnyFreeSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)
  private def run(params: String, state: String = "{}") = new RespondNode[Id].run(NodeInput(json(params), json(state)))

  "it finishes when it has nothing to check, or when the body is in the state" in {
    run("{}").failed shouldBe false
    run("""{"status":201,"body":"order"}""", """{"order":{"id":1}}""").failed shouldBe false
  }

  "a body that is not in the state fails through the error port" in {
    val out = run("""{"body":"order"}""")
    out.port shouldBe Port.Error
    out.error.get should include("'order'")
  }

  "bad configuration fails, and the description checks it" in {
    run("""{"status":"ok"}""").failed shouldBe true
    val d = RespondNode.descriptor
    d.config.validate(json("""{"status":201,"body":"a.b","headers":{"X":"y"}}""")) shouldBe Nil
    d.checkParams(json("""{"status":42}""")).size shouldBe 1
    d.checkParams(json("""{"body":"a..b"}""")).size shouldBe 1
    d.reads(json("""{"body":"a.b"}""")) shouldBe List(StateRead(StatePath(List("a", "b")), DataType.AnyJson))
    d.reads(json("{}")) shouldBe Nil
  }

  "it is in the catalog and the registry of the core nodes" in {
    CoreNodes.catalog.get(RespondNode.Kind) should not be empty
  }
}
