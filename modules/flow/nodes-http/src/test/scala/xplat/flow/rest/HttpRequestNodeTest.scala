package xplat.flow.rest

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._
import xplat.flow.nodes.Connections

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentLinkedQueue

class HttpRequestNodeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, identity)

  /** What the server saw: `METHOD path?query | authorization | body`. */
  private final case class Server(url: String, seen: ConcurrentLinkedQueue[String])

  private val server: Resource[IO, Server] = Resource.make(IO {
    val seen = new ConcurrentLinkedQueue[String]()
    val s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    s.createContext("/", (ex: HttpExchange) => {
      val body = new String(ex.getRequestBody.readAllBytes(), UTF_8)
      seen.add(s"${ex.getRequestMethod} ${ex.getRequestURI} | ${Option(ex.getRequestHeaders.getFirst("Authorization")).getOrElse("-")} | $body")
      val (code, out, kind) = ex.getRequestURI.getPath match {
        case "/missing" => (404, """{"error":"nope"}""", "application/json")
        case "/text"    => (200, "hello", "text/plain")
        case _          => (200, """{"ok":true,"items":[1,2]}""", "application/json")
      }
      ex.getResponseHeaders.add("Content-Type", kind)
      val bytes = out.getBytes(UTF_8)
      ex.sendResponseHeaders(code, bytes.length.toLong)
      ex.getResponseBody.write(bytes)
      ex.close()
    })
    s.start()
    s -> Server(s"http://127.0.0.1:${s.getAddress.getPort}", seen)
  })(s => IO(s._1.stop(0))).map(_._2)

  private def node(conns: Map[String, Connections.Resolved] = Map.empty) = new HttpRequestNode[IO]((n: String) => IO.pure(conns.get(n)))

  private def input(params: Json, state: String = "{}", outputs: List[String] = Nil): NodeInput =
    NodeInput(params, json(state), instanceId = "i1", nodeId = "n1", outputs = outputs.map(n => OutputDecl(n, StatePath(List(n)), DataType.AnyJson)))

  "a GET with values of the state in the url and the query answers the parsed JSON" in server.use { s =>
    val params = Json.obj("url" -> Json.fromString(s"${s.url}/users/{{user.id}}"), "query" -> json("""{"q": {"$path": "user.name"}, "limit": 5, "none": null}"""))
    node().run(input(params, """{"user": {"id": 7, "name": "Ana Pérez"}}""", List("body", "status"))).map { out =>
      out.error shouldBe None
      out.values shouldBe Map("body" -> json("""{"ok":true,"items":[1,2]}"""), "status" -> Json.fromInt(200))
      s.seen.peek() shouldBe "GET /users/7?q=Ana%20P%C3%A9rez&limit=5 | - | "
    }
  }

  "a POST sends its body with markers, and a connection gives the base url and the token" in server.use { s =>
    val conn = Connections.Resolved("api", "http", Json.obj("baseUrl" -> Json.fromString(s.url + "/")), Map("token" -> "s3cret"))
    val params = json("""{"method": "post", "url": "/orders", "connection": "api", "body": {"id": {"$path": "order.id"}, "key": {"$context": "key"}}}""")
    node(Map("api" -> conn)).run(input(params, """{"order": {"id": 9}}""")).map { out =>
      out.error shouldBe None
      s.seen.peek() shouldBe """POST /orders | Bearer s3cret | {"id":9,"key":"i1:n1"}"""
      out.info("request").hcursor.downField("headers").get[String]("Authorization") shouldBe Right("***")
    }
  }

  "a status that is not 2xx fails, unless failOnStatus is false; a body that is not JSON is text" in server.use { s =>
    for {
      failed <- node().run(input(json(s"""{"url": "${s.url}/missing"}""")))
      kept <- node().run(input(json(s"""{"url": "${s.url}/missing", "failOnStatus": false}"""), outputs = List("status", "body")))
      text <- node().run(input(json(s"""{"url": "${s.url}/text"}"""), outputs = List("body")))
    } yield {
      failed.error.get should include("answered 404")
      kept.values shouldBe Map("status" -> Json.fromInt(404), "body" -> json("""{"error":"nope"}"""))
      text.values("body") shouldBe Json.fromString("hello")
    }
  }

  "the configuration is checked, and test mode does not call" in IO {
    HttpRequestNode.descriptor.checkParams(json("""{"url": "ftp://x"}""")) should not be empty
    HttpRequestNode.descriptor.checkParams(json("""{"url": "https://x", "method": "GET", "body": {"a": 1}}""")) should not be empty
    HttpRequestNode.descriptor.reads(json("""{"url": "https://x/{{a.b}}", "query": {"c": {"$path": "c"}}}""")).map(_.path.toString) shouldBe List("a.b", "c")
    val sim = node().simulated(input(json("""{"url": "https://unreachable.invalid/x"}"""))).get
    sim.failed shouldBe false
    sim.info("simulated") shouldBe Json.True
  }

  "a value of the url that is not in the state fails the node" in {
    node().run(input(json("""{"url": "https://x.test/{{missing}}"}"""))).map(_.error.get should include("not in the state"))
  }
}
