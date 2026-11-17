package xplat.flow.stream

import io.circe.Json
import xplat.flow.model._

import java.nio.charset.StandardCharsets.UTF_8

class DeadLetterRoutesTest extends StreamSpec {

  private val flow = FlowDef(
    "f",
    "F",
    List(
      NodeDef("q", QueueSource.Kind, json("""{"connection": "broker", "source": "orders"}""")),
      NodeDef("l", ListSource.Kind, json("""{"path": "xs"}"""))
    )
  )

  private val audit = json("""{"instance": {}, "entries": [
    {"kind": "node-started", "node": "q"},
    {"kind": "node-finished", "node": "q", "info": {"deadLetters": [
      {"node": "m", "error": "bad", "element": {"a": 2}, "origin": {"a": 1}, "source": "q"},
      {"node": "m", "error": "bad", "element": 3, "origin": 3, "source": "l"},
      {"node": "w", "error": "bad", "element": {"items": []}}
    ]}}
  ]}""")

  "the dead letters of an instance are numbered, and only those from a queue with their original can be published again" in {
    val ls = DeadLetterRoutes.letters(audit, Some(flow))
    ls.map(l => (l.index, l.segment, l.reinjectable)) shouldBe List((0, "q", true), (1, "q", false), (2, "q", false))
    ls.head.origin shouldBe Some(json("""{"a": 1}"""))
    ls.head.toJson.hcursor.get[String]("sourceKind") shouldBe Right(QueueSource.Kind)
  }

  "the message published again is the original, as the source read it" in {
    val plain = QueueSource.params(json("""{"connection": "c", "source": "s"}""")).toOption.get
    val envelope = QueueSource.params(json("""{"connection": "c", "source": "s", "envelope": true}""")).toOption.get
    val text = QueueSource.params(json("""{"connection": "c", "source": "s", "format": "text"}""")).toOption.get
    DeadLetterRoutes.message(plain, json("""{"a": 1}""")).map { case (b, k) => (new String(b, UTF_8), k) } shouldBe Right(("""{"a":1}""", None))
    DeadLetterRoutes.message(envelope, json("""{"key": "k1", "body": {"a": 1}}""")).map { case (b, k) => (new String(b, UTF_8), k) } shouldBe Right(("""{"a":1}""", Some("k1")))
    DeadLetterRoutes.message(text, Json.fromString("hola")).map { case (b, k) => (new String(b, UTF_8), k) } shouldBe Right(("hola", None))
    DeadLetterRoutes.message(text, json("{}")).isLeft shouldBe true
  }
}
