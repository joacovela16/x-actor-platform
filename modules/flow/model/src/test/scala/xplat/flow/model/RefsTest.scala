package xplat.flow.model

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class RefsTest extends AnyFreeSpec with Matchers {

  private def json(t: String): Json = parse(t).fold(e => throw e, identity)

  private val state = json("""{"qty": 3, "user": {"id": "u 1", "tags": ["a"]}, "empty": ""}""")
  private val lookup = Refs.lookup("state" -> Refs.at(state))

  "parsing" - {
    "finds the references, with origin, path and default" in {
      Refs.parse("a ${state.user.id} b ${env.PORT:-27017} $${not}") shouldBe
        Right(List(Refs.Ref("state", List("user", "id"), None), Refs.Ref("env", List("PORT"), Some("27017"))))
    }
    "a broken reference is a problem" in {
      Refs.parse("${state..x}").isLeft shouldBe true
      Refs.parse("${}").isLeft shouldBe true
    }
    "a whole reference is only one reference and nothing else" in {
      Refs.whole("${state.qty}") shouldBe Some(Refs.Ref("state", List("qty"), None))
      Refs.whole("x${state.qty}") shouldBe None
      Refs.whole("${state.qty}${state.qty}") shouldBe None
    }
  }

  "resolving" - {
    "a whole reference keeps the type of the value" in {
      Refs.resolveText("${state.qty}", lookup) shouldBe Right(Json.fromInt(3))
      Refs.resolveText("${state.user.tags}", lookup) shouldBe Right(json("""["a"]"""))
    }
    "inside a text it is text, through escape" in {
      Refs.resolveText("n=${state.qty}, id=${state.user.id}", lookup) shouldBe Right(Json.fromString("n=3, id=u 1"))
      Refs.resolveText("/u/${state.user.id}", lookup, _.replace(" ", "%20")) shouldBe Right(Json.fromString("/u/u%201"))
    }
    "a default is used when the value is missing, null or empty" in {
      Refs.resolveText("${state.nope:-x}-${state.empty:-y}", lookup) shouldBe Right(Json.fromString("x-y"))
    }
    "a missing value without default, or an origin not given, is a problem" in {
      Refs.resolveText("${state.nope}", lookup).isLeft shouldBe true
      Refs.resolveText("${env.HOME}", lookup).left.toOption.get should include("cannot be used here")
    }
    "$${ is a literal" in {
      Refs.resolveText("$${state.qty} is ${state.qty}", lookup) shouldBe Right(Json.fromString("${state.qty} is 3"))
    }
    "every text of a document" in {
      Refs.resolve(json("""{"q": {"n": "${state.qty}", "in": ["${state.user.id}", 1]}, "$match": true}"""), lookup) shouldBe
        Right(json("""{"q": {"n": 3, "in": ["u 1", 1]}, "$match": true}"""))
    }
  }

  "problems say which origins a place does not allow" in {
    Refs.problems(json("""{"a": "${env.X}", "b": "${state.y}"}"""), Set("state"), "a node") shouldBe
      List("'${env.X}': 'env' cannot be used in a node (only state.)")
  }
}
