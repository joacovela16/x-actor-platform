package xplat.flow.ai

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import io.circe.Json
import io.circe.parser.parse
import org.http4s._
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString
import xplat.flow.model._
import xplat.flow.nodes.Connections

import java.nio.file.Files
import scala.concurrent.duration._

class AiNodeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)

  private val apiKey = "sk-secret-key-123"

  private val claude = Connections.Resolved("claude", AiKinds.Claude, json("""{"baseUrl": "https://ai.test", "model": "claude-sonnet-5-5"}"""), Map("apiKey" -> apiKey))
  private val openai = Connections.Resolved("gpt", AiKinds.OpenAi, json("""{"baseUrl": "https://oa.test/v1", "model": "gpt-x"}"""), Map("apiKey" -> apiKey))
  private val registry: Connections[IO] = (name: String) => IO.pure(List(claude, openai).find(_.name == name))

  /** What the fake service saw. */
  private final case class Seen(url: String, headers: Map[String, String], body: Json)

  private def anthropicAnswer(text: String, in: Int = 11, out: Int = 7): Json =
    json(s"""{"content": [{"type": "text", "text": ${Json.fromString(text).noSpaces}}], "usage": {"input_tokens": $in, "output_tokens": $out}, "stop_reason": "end_turn"}""")

  private def openaiAnswer(text: String): Json =
    json(s"""{"choices": [{"message": {"content": ${Json.fromString(text).noSpaces}}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 5, "completion_tokens": 3}}""")

  /** A service that gives the answers in order (the last one repeats) and records the requests. */
  private final case class Svc(client: Client[IO], seen: Ref[IO, Vector[Seen]])

  private def service(answers: IO[Response[IO]]*): IO[Svc] =
    for {
      seen <- Ref.of[IO, Vector[Seen]](Vector.empty)
      n <- Ref.of[IO, Int](0)
      app = HttpApp[IO] { request =>
        for {
          body <- request.as[Json]
          _ <- seen.update(_ :+ Seen(request.uri.renderString, request.headers.headers.map(h => h.name.toString.toLowerCase -> h.value).toMap, body))
          i <- n.getAndUpdate(_ + 1)
          response <- answers(math.min(i, answers.size - 1))
        } yield response
      }
    } yield Svc(Client.fromHttpApp(app), seen)

  private def ok(body: Json): IO[Response[IO]] = Ok(body)

  private def node(client: Client[IO], settings: AiNode.Settings = AiNode.Settings(backoff = 5.millis)) = new AiNode[IO](registry, AiTransport.http(client), settings)

  private def exec(n: AiNode[IO], params: String, state: String = "{}", outputs: List[String] = Nil): IO[NodeOutput] =
    n.run(NodeInput(json(params), json(state), outputs = outputs.map(o => OutputDecl(o, StatePath(List(o)), DataType.AnyJson))))

  "Claude" - {

    "asks the Messages API and writes the text, with the use of tokens in the audit" in {
      for {
        svc1 <- service(ok(anthropicAnswer("Hello Ana")))
        client = svc1.client
        seen = svc1.seen
        out <- exec(node(client), """{"connection": "claude", "system": "Be brief", "prompt": "Greet {{user.name}}", "maxTokens": 50, "temperature": 0.2}""", """{"user": {"name": "Ana"}}""", List("text"))
        requests <- seen.get
      } yield {
        out.values shouldBe Map("text" -> Json.fromString("Hello Ana"))
        out.info("usage") shouldBe json("""{"inputTokens": 11, "outputTokens": 7}""")
        out.info("model") shouldBe Json.fromString("claude-sonnet-5-5")
        out.info("attempts") shouldBe Json.fromInt(1)
        out.info("stop") shouldBe Json.fromString("end_turn")
        requests.size shouldBe 1
        val r = requests.head
        r.url shouldBe "https://ai.test/v1/messages"
        r.headers("x-api-key") shouldBe apiKey
        r.headers("anthropic-version") shouldBe "2023-06-01"
        r.body.hcursor.get[String]("model") shouldBe Right("claude-sonnet-5-5")
        r.body.hcursor.get[Int]("max_tokens") shouldBe Right(50)
        r.body.hcursor.get[String]("system") shouldBe Right("Be brief")
        r.body.hcursor.get[Double]("temperature") shouldBe Right(0.2)
        r.body.hcursor.downField("messages").downArray.get[String]("content") shouldBe Right("Greet Ana")
      }
    }

    "the model of the node wins over the one of the connection" in {
      for {
        svc2 <- service(ok(anthropicAnswer("x")))
        client = svc2.client
        seen = svc2.seen
        _ <- exec(node(client), """{"connection": "claude", "prompt": "hi", "model": "claude-haiku-5-5"}""")
        requests <- seen.get
      } yield requests.head.body.hcursor.get[String]("model") shouldBe Right("claude-haiku-5-5")
    }

    "a connection that names no model uses the default one of its kind" in {
      val bare = Connections.Resolved("bare", AiKinds.Claude, Json.obj(), Map("apiKey" -> apiKey))
      val reg: Connections[IO] = (_: String) => IO.pure(Some(bare))
      for {
        svc <- service(ok(anthropicAnswer("x")))
        _ <- exec(new AiNode[IO](reg, AiTransport.http(svc.client)), """{"connection": "bare", "prompt": "hi"}""")
        requests <- svc.seen.get
      } yield {
        requests.head.url shouldBe "https://api.anthropic.com/v1/messages"
        requests.head.body.hcursor.get[String]("model") shouldBe Right("claude-sonnet-5-5")
      }
    }

    "the key is never in the audit" in {
      for {
        svc3 <- service(ok(anthropicAnswer("x")))
        client = svc3.client
        out <- exec(node(client), """{"connection": "claude", "prompt": "hi"}""")
      } yield out.info.values.map(_.noSpaces).mkString should not include apiKey
    }
  }

  "OpenAI-compatible" - {

    "asks the Chat Completions API" in {
      for {
        svc4 <- service(ok(openaiAnswer("42")))
        client = svc4.client
        seen = svc4.seen
        out <- exec(node(client), """{"connection": "gpt", "system": "Math", "prompt": "6*7?"}""", outputs = List("text"))
        requests <- seen.get
      } yield {
        out.values shouldBe Map("text" -> Json.fromString("42"))
        out.info("usage") shouldBe json("""{"inputTokens": 5, "outputTokens": 3}""")
        val r = requests.head
        r.url shouldBe "https://oa.test/v1/chat/completions"
        r.headers("authorization") shouldBe s"Bearer $apiKey"
        val messages = r.body.hcursor.downField("messages").as[List[Json]].toOption.get
        messages.map(_.hcursor.get[String]("role").toOption.get) shouldBe List("system", "user")
        r.body.hcursor.downField("response_format").focus shouldBe None
      }
    }

    "json mode asks for a json object" in {
      for {
        svc5 <- service(ok(openaiAnswer("""{"ok": true}""")))
        client = svc5.client
        seen = svc5.seen
        out <- exec(node(client), """{"connection": "gpt", "prompt": "give json", "json": true}""", outputs = List("json"))
        requests <- seen.get
      } yield {
        out.values shouldBe Map("json" -> json("""{"ok": true}"""))
        requests.head.body.hcursor.downField("response_format").get[String]("type") shouldBe Right("json_object")
      }
    }
  }

  "json answers" - {

    "fenced or surrounded JSON is found, and it is checked against the schema" in {
      val schema = """{"type": "obj", "fields": {"label": {"type": "str"}, "score": {"type": "num"}}}"""
      for {
        svc6 <- service(ok(anthropicAnswer("Sure!\n```json\n{\"label\": \"spam\", \"score\": 0.9}\n```")))
        client = svc6.client
        good <- exec(node(client), s"""{"connection": "claude", "prompt": "classify", "json": true, "schema": $schema}""", outputs = List("json"))
        svc7 <- service(ok(anthropicAnswer("""{"label": "spam", "score": "high"}""")))
        client2 = svc7.client
        wrong <- exec(node(client2), s"""{"connection": "claude", "prompt": "classify", "json": true, "schema": $schema}""")
        svc8 <- service(ok(anthropicAnswer("I cannot do that")))
        client3 = svc8.client
        notJson <- exec(node(client3), """{"connection": "claude", "prompt": "classify", "json": true}""")
      } yield {
        good.values shouldBe Map("json" -> json("""{"label": "spam", "score": 0.9}"""))
        wrong.port shouldBe Port.Error
        wrong.error.get should include("does not fit")
        notJson.port shouldBe Port.Error
        notJson.error.get should include("not valid JSON")
        wrong.info should contain key "usage"
      }
    }

    "text without json: true has no json output to declare" in {
      for {
        svc9 <- service(ok(anthropicAnswer("hi")))
        client = svc9.client
        out <- exec(node(client), """{"connection": "claude", "prompt": "hi"}""", outputs = List("json"))
      } yield {
        out.failed shouldBe true
        out.error.get should include("cannot answer 'json'")
      }
    }

    "a node that declares usage gets it, and one that declares nothing gets the text" in {
      for {
        svc10 <- service(ok(anthropicAnswer("hi")))
        client = svc10.client
        usage <- exec(node(client), """{"connection": "claude", "prompt": "hi"}""", outputs = List("text", "usage"))
        bare <- exec(node(client), """{"connection": "claude", "prompt": "hi"}""")
      } yield {
        usage.values("usage") shouldBe json("""{"inputTokens": 11, "outputTokens": 7}""")
        bare.values.keySet shouldBe Set("text")
      }
    }
  }

  "errors and retries" - {

    "a 429 or a 5xx is retried, and the answer comes at the end" in {
      for {
        svc11 <- service(TooManyRequests("slow down"), InternalServerError("oops"), ok(anthropicAnswer("finally")))
        client = svc11.client
        seen = svc11.seen
        out <- exec(node(client), """{"connection": "claude", "prompt": "hi", "retries": 3}""")
        requests <- seen.get
      } yield {
        out.values("text") shouldBe Json.fromString("finally")
        out.info("attempts") shouldBe Json.fromInt(3)
        requests.size shouldBe 3
      }
    }

    "Retry-After is obeyed" in {
      for {
        svc12 <- service(TooManyRequests("wait").map(_.putHeaders(Header.Raw(CIString("Retry-After"), "0"))), ok(anthropicAnswer("ok")))
        client = svc12.client
        started <- IO.monotonic
        out <- exec(node(client, AiNode.Settings(backoff = 5.seconds)), """{"connection": "claude", "prompt": "hi"}""")
        took <- IO.monotonic.map(_ - started)
      } yield {
        out.values("text") shouldBe Json.fromString("ok")
        took should be < 3.seconds
      }
    }

    "it gives up after the retries, with what the service said" in {
      for {
        svc13 <- service(InternalServerError(json("""{"error": {"message": "overloaded"}}""")))
        client = svc13.client
        seen = svc13.seen
        out <- exec(node(client), """{"connection": "claude", "prompt": "hi", "retries": 2}""")
        requests <- seen.get
      } yield {
        out.port shouldBe Port.Error
        out.error.get should (include("500") and include("overloaded"))
        requests.size shouldBe 3
      }
    }

    "an error of the request is not retried, and the key is hidden even if the service repeats it" in {
      for {
        svc14 <- service(BadRequest(json(s"""{"error": {"message": "invalid key $apiKey"}}""")))
        client = svc14.client
        seen = svc14.seen
        out <- exec(node(client), """{"connection": "claude", "prompt": "hi", "retries": 5}""")
        requests <- seen.get
      } yield {
        out.failed shouldBe true
        out.error.get should include("400")
        out.error.get should not include apiKey
        out.error.get should include("***")
        requests.size shouldBe 1
      }
    }

    "a service that does not answer in time fails the node" in {
      for {
        svc15 <- service(IO.sleep(5.seconds) *> ok(anthropicAnswer("late")))
        client = svc15.client
        out <- exec(node(client), """{"connection": "claude", "prompt": "hi", "timeoutSeconds": 1, "retries": 0}""")
      } yield {
        out.port shouldBe Port.Error
        out.error.get should include("did not answer in 1 seconds")
      }
    }

    "a connection that is missing, of another kind or without key fails clearly" in {
      val other = Connections.Resolved("kafka", "kafka", Json.obj(), Map.empty)
      val noKey = Connections.Resolved("nokey", AiKinds.Claude, Json.obj(), Map.empty)
      val reg: Connections[IO] = (name: String) => IO.pure(List(other, noKey).find(_.name == name))
      for {
        svc16 <- service(ok(anthropicAnswer("x")))
        client = svc16.client
        seen = svc16.seen
        n = new AiNode[IO](reg, AiTransport.http(client))
        missing <- exec(n, """{"connection": "nowhere", "prompt": "hi"}""")
        wrongKind <- exec(n, """{"connection": "kafka", "prompt": "hi"}""")
        withoutKey <- exec(n, """{"connection": "nokey", "prompt": "hi"}""")
        requests <- seen.get
      } yield {
        missing.error.get should include("does not exist")
        wrongKind.error.get should include("not an AI service")
        withoutKey.error.get should include("no apiKey")
        requests shouldBe empty
      }
    }
  }

  "prompts and budget" - {

    "{{path}} puts values of the state in: strings as they are, the rest as JSON" in {
      for {
        svc17 <- service(ok(anthropicAnswer("x")))
        client = svc17.client
        seen = svc17.seen
        _ <- exec(node(client), """{"connection": "claude", "prompt": "Name {{a}}, items {{b}}, n {{ c.d }}"}""", """{"a": "Ana", "b": [1, 2], "c": {"d": 3}}""")
        requests <- seen.get
      } yield requests.head.body.hcursor.downField("messages").downArray.get[String]("content") shouldBe Right("Name Ana, items [1,2], n 3")
    }

    "a value that is not in the state fails before calling the service" in {
      for {
        svc18 <- service(ok(anthropicAnswer("x")))
        client = svc18.client
        seen = svc18.seen
        out <- exec(node(client), """{"connection": "claude", "prompt": "Hi {{missing}}"}""")
        requests <- seen.get
      } yield {
        out.error.get should include("'missing' is not in the state")
        requests shouldBe empty
      }
    }

    "a prompt that is clearly larger than the budget is not sent, and the answer is shortened to what is left" in {
      for {
        svc19 <- service(ok(anthropicAnswer("x")))
        client = svc19.client
        seen = svc19.seen
        refused <- exec(node(client), s"""{"connection": "claude", "prompt": "${"word " * 400}", "maxTotalTokens": 100}""")
        _ <- exec(node(client), s"""{"connection": "claude", "prompt": "${"word " * 100}", "maxTotalTokens": 200, "maxTokens": 4000}""")
        requests <- seen.get
      } yield {
        refused.error.get should include("more than maxTotalTokens")
        requests.size shouldBe 1
        // about 125 tokens of prompt, so at most 75 are left for the answer
        requests.head.body.hcursor.get[Int]("max_tokens").toOption.get should be <= 80
      }
    }

    "a run that used more than the budget fails, and its use is still in the audit" in {
      for {
        svc20 <- service(ok(anthropicAnswer("x", in = 150, out = 80)))
        client = svc20.client
        out <- exec(node(client), """{"connection": "claude", "prompt": "hi", "maxTotalTokens": 200}""")
      } yield {
        out.failed shouldBe true
        out.error.get should include("230 tokens")
        out.info("usage") shouldBe json("""{"inputTokens": 150, "outputTokens": 80}""")
      }
    }
  }

  "test mode" - {

    "it does not call the service: it answers the simulated response, and shows the prompt it would have sent" in {
      for {
        svc21 <- service(ok(anthropicAnswer("real")))
        client = svc21.client
        seen = svc21.seen
        n = node(client)
        out = n.simulated(NodeInput(json("""{"connection": "claude", "system": "S {{a}}", "prompt": "P {{a}}", "simulatedResponse": "fixed"}"""), json("""{"a": "1"}"""), outputs = List(OutputDecl("text", StatePath(List("t")), DataType.Str)))).get
        requests <- seen.get
      } yield {
        out.values shouldBe Map("text" -> Json.fromString("fixed"))
        out.info("simulated") shouldBe Json.True
        out.info("prompt") shouldBe Json.fromString("P 1")
        out.info("system") shouldBe Json.fromString("S 1")
        requests shouldBe empty
      }
    }

    "in json mode it answers a value of the schema, so the declared type is met" in {
      val n = node(Client.fromHttpApp(HttpApp.notFound[IO]))
      val out = n.simulated(NodeInput(json("""{"connection": "claude", "prompt": "p", "json": true, "schema": {"type": "obj", "fields": {"ok": {"type": "bool"}, "tags": {"type": "list", "element": {"type": "str"}}}}}"""), json("{}"), outputs = List(OutputDecl("json", StatePath(List("j")), DataType.AnyJson)))).get
      IO(out.values("json") shouldBe json("""{"ok": false, "tags": []}"""))
    }

    "a configuration with problems fails in the simulation too" in {
      val n = node(Client.fromHttpApp(HttpApp.notFound[IO]))
      IO(n.simulated(NodeInput(json("""{"connection": "claude"}"""), json("{}"))).get.failed shouldBe true)
    }
  }

  "recordings" - {

    "a recorded answer can be played back without the service" in {
      val dir = Files.createTempDirectory("flow-ai-cassettes")
      val params = """{"connection": "claude", "prompt": "Greet {{n}}"}"""
      for {
        svc22 <- service(ok(anthropicAnswer("recorded hello")))
        client = svc22.client
        seen = svc22.seen
        recording = new AiNode[IO](registry, Cassettes(AiTransport.http(client), Cassettes.Mode.Record(dir)))
        first <- exec(recording, params, """{"n": "Ana"}""")
        // no service at all now
        replaying = new AiNode[IO](registry, Cassettes(AiTransport.http(Client.fromHttpApp(HttpApp.notFound[IO])), Cassettes.Mode.Replay(dir)))
        again <- exec(replaying, params, """{"n": "Ana"}""")
        unknown <- exec(replaying, params, """{"n": "Luis"}""")
        requests <- seen.get
      } yield {
        first.values("text") shouldBe Json.fromString("recorded hello")
        again.values("text") shouldBe Json.fromString("recorded hello")
        unknown.port shouldBe Port.Error
        unknown.error.get should include("no recorded answer")
        requests.size shouldBe 1
        // the key is not in what was written
        Files.list(dir).toArray.map(p => new String(Files.readAllBytes(p.asInstanceOf[java.nio.file.Path]))).mkString should not include apiKey
      }
    }

    "answers that were not a success are not recorded" in {
      val dir = Files.createTempDirectory("flow-ai-cassettes")
      for {
        svc23 <- service(InternalServerError("no"))
        client = svc23.client
        recording = new AiNode[IO](registry, Cassettes(AiTransport.http(client), Cassettes.Mode.Record(dir)))
        _ <- exec(recording, """{"connection": "claude", "prompt": "x", "retries": 0}""")
      } yield (if (Files.exists(dir)) Files.list(dir).count() else 0L) shouldBe 0L
    }
  }

  "configuration" - {

    "it is checked when the flow is saved" in IO {
      def check(p: String) = AiNode.descriptor.checkParams(json(p))
      check("""{"connection": "c", "prompt": "hi"}""") shouldBe Nil
      check("""{"connection": "c"}""").head should include("prompt")
      check("""{"connection": "c", "prompt": "hi", "maxTokens": 0}""").head should include("maxTokens")
      check("""{"connection": "c", "prompt": "hi", "temperature": 3}""").head should include("temperature")
      check("""{"connection": "c", "prompt": "hi", "retries": 99}""").head should include("retries")
      check("""{"connection": "c", "prompt": "hi", "schema": {"type": "str"}}""").head should include("schema only applies")
      check("""{"connection": "c", "prompt": "hi {{a..b}}"}""").head should include("not a valid state path")
      AiNode.descriptor.config.validate(json("""{"connection": "c", "prompt": "hi"}""")) shouldBe Nil
    }

    "it reads what the prompts use, and writes text by default" in IO {
      AiNode.descriptor.reads(json("""{"connection": "c", "system": "{{s}}", "prompt": "{{a.b}} and {{a.b}}"}""")).map(_.path.render) shouldBe List("a.b", "s")
      AiNode.descriptor.defaultOutputs(Json.obj()).map(_.name) shouldBe List("text")
      AiNode.descriptor.sideEffects shouldBe SideEffects.Read
    }

    "the kinds of connection keep the key secret" in IO {
      AiNodes.connectionKinds.map(_.kind) shouldBe List("claude", "openai", "codex")
      AiNodes.connectionKinds.foreach(k => k.config.fields.find(_.name == "apiKey").map(_.secret) shouldBe Some(true))
      AiNodes.catalog.all.map(_.kind) shouldBe List("ai.agent")
    }

    "the validator finds the values the prompt needs" in IO {
      val flow = FlowDef("f", "F", nodes = List(NodeDef("a", AiNode.Kind, json("""{"connection": "c", "prompt": "Hi {{who}}"}"""))))
      FlowValidator.validate(flow, AiNodes.catalog).map(_.code) shouldBe List("state-not-declared")
    }
  }
}
