package xplat.flow.http

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class HookRequestTest extends AnyFreeSpec with Matchers {

  private def json(t: String): Json = parse(t).fold(e => throw e, identity)

  private val request = HookRequest.json("GET", "test", Map("urn" -> "ipa:fline:1", "n" -> "7"), Map("x-user" -> "ana"), json("""{"order": {"id": 5}}"""))
  private val env: EnvRefs.Env = Map("REGION" -> "uy").get

  private def http(inputs: (String, String)*) =
    TriggerDef("t", "f", TriggerDef.Kind.Http, path = Some("test"), inputs = inputs.map { case (k, v) => k -> Json.fromString(v) }.toMap)

  "http inputs with ${request.*} and ${env.*}; a whole reference keeps its type" in {
    HookRequest.inputs(http("urn" -> "${request.query.urn}", "id" -> "${request.body.order.id}", "who" -> "${request.headers.x-user}@${env.REGION}"), request, env) shouldBe
      json("""{"urn": "ipa:fline:1", "id": 5, "who": "ana@uy"}""")
  }

  "the older $query.x still works, and a missing value leaves the input out" in {
    HookRequest.inputs(http("urn" -> "$query.urn", "nope" -> "${request.query.nope}", "fixed" -> "x"), request, env) shouldBe
      json("""{"urn": "ipa:fline:1", "fixed": "x"}""")
  }

  "queue inputs read ${message.*}" in {
    val msg = HookRequest.message("orders", Some("k1"), "42", Map.empty, json("""{"a": 1}"""))
    val t = TriggerDef("q", "f", TriggerDef.Kind.Queue, queue = Some("orders"), connection = Some("c"), inputs = Map("a" -> Json.fromString("${message.body.a}"), "k" -> Json.fromString("${message.key}")))
    HookRequest.inputs(t, msg, env) shouldBe json("""{"a": 1, "k": "k1"}""")
  }

  "a trigger refuses origins it does not have" in {
    http("x" -> "${state.urn}").problems.head should include("input 'x'")
    http("x" -> "${message.body}").problems.head should include("'message' cannot be used in a http trigger")
    http("x" -> "${request.query.urn}", "y" -> "${env.REGION}").problems shouldBe Nil
  }
}
