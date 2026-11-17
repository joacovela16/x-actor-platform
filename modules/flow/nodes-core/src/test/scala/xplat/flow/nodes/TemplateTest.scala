package xplat.flow.nodes

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._

class TemplateTest extends AnyFreeSpec with Matchers {

  private def json(t: String): Json = parse(t).fold(e => throw e, identity)

  private val input = NodeInput(params = Json.obj(), state = json("""{"urn": "ipa:fline:1", "qty": 2}"""), instanceId = "i1", nodeId = "n1")

  "the unified references and the older markers give the same" in {
    Template.resolve(json("""{"urn": "${state.urn}", "n": "${state.qty}", "_id": "${context.key}"}"""), input) shouldBe
      Right(json("""{"urn": "ipa:fline:1", "n": 2, "_id": "i1:n1"}"""))
    Template.resolve(json("""{"urn": {"$path": "urn"}, "n": {"$path": "qty"}, "_id": {"$context": "key"}}"""), input) shouldBe
      Right(json("""{"urn": "ipa:fline:1", "n": 2, "_id": "i1:n1"}"""))
  }

  "reads are the state paths without a default" in {
    Template.reads(json("""{"a": "${state.urn}", "b": ["x ${state.order.id}"], "c": "${state.opt:-1}", "d": {"$path": "qty"}}""")).map(_.render).toSet shouldBe
      Set("urn", "order.id", "qty")
  }

  "problems: origins a node cannot use, unknown contexts, a state without path" in {
    Template.problems(json("""{"a": "${env.SECRET}"}""")).head should include("'env' cannot be used in a node")
    Template.problems(json("""{"a": "${context.flowName}"}""")).head should include("context is one of")
    Template.problems(json("""{"a": "${state}"}""")).head should include("needs a path")
    Template.problems(json("""{"a": "${state.urn}", "b": "${context.instanceId}"}""")) shouldBe Nil
  }

  "a text, with its values escaped" in {
    Template.text("/p/${state.urn}", input, _.replace(":", "%3A")) shouldBe Right("/p/ipa%3Afline%3A1")
  }
}
