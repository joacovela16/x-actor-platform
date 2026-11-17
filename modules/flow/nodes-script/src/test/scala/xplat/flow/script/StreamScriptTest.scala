package xplat.flow.script

import cats.effect.{IO, Ref}
import io.circe.Json
import xplat.flow.model._
import xplat.flow.stream.StreamSpec

import scala.concurrent.duration._

class StreamScriptTest extends StreamSpec {

  private def op(limits: ScriptSandbox.Limits = ScriptSandbox.Limits()) = new StreamScript[IO](limits)

  private def params(code: String, mode: String, extra: (String, Json)*): Json =
    Json.obj(("code" -> Json.fromString(code)) +: ("mode" -> Json.fromString(mode)) +: extra: _*)

  private def run(code: String, mode: String, values: Json*) = runOperator(op(), StreamScript.Kind, params(code, mode), plain(values: _*))

  "map returns the new element" in {
    // A field that is undefined is not in the JSON.
    run("return { n: element.n * 2, tag: params.tag };", "map", json("""{"n": 1}"""), json("""{"n": 2}""")).map(_._1 shouldBe List(json("""{"n": 2}"""), json("""{"n": 4}""")))
  }

  "map sees the args as params" in {
    runOperator(op(), StreamScript.Kind, params("return element.n + params.k;", "map", "args" -> json("""{"k": 10}""")), plain(json("""{"n": 1}"""))).map(_._1 shouldBe numbers(11))
  }

  "filter keeps the elements for which it is true and drops the rest, counted" in {
    for {
      acks <- Ref.of[IO, Int](0)
      r <- runOperator(op(), StreamScript.Kind, params("return element.n > 10;", "filter"), elements(acks, json("""{"n": 5}"""), json("""{"n": 50}"""), json("""{"n": 11}""")))
      (out, h) = r
      n <- acks.get
      m <- h.metrics.snapshot
    } yield {
      out.map(_.hcursor.get[Int]("n").toOption) shouldBe List(Some(50), Some(11))
      n shouldBe 1
      m.hcursor.get[Int]("dropped") shouldBe Right(1)
    }
  }

  "filter treats what is true in JavaScript as true" in {
    run("return element.v;", "filter", json("""{"v": 0}"""), json("""{"v": "x"}"""), json("""{"v": ""}"""), json("""{"v": [1]}"""), json("""{"v": null}""")).map(_._1.map(_.hcursor.get[Json]("v").toOption.get) shouldBe List(Json.fromString("x"), json("[1]")))
  }

  "flatMap gives one element for each item of the list; an empty list drops the element" in {
    run("return element.items;", "flatMap", json("""{"items": [1, 2, 3]}"""), json("""{"items": []}"""), json("""{"items": [4]}""")).map(_._1 shouldBe numbers(1, 2, 3, 4))
  }

  "an element where the script throws, returns the wrong thing or returns nothing is a dead letter, and the stream goes on" in {
    for {
      r <- run("if (element.n === 2) throw new Error('bad two'); if (element.n === 3) return; return element.n;", "map", json("""{"n": 1}"""), json("""{"n": 2}"""), json("""{"n": 3}"""), json("""{"n": 4}"""))
      (out, h) = r
      dead <- h.dead.get
      m <- h.metrics.snapshot
    } yield {
      out shouldBe numbers(1, 4)
      dead.map(_.hcursor.get[String]("error").toOption.get) shouldBe Vector("line 1: Error: bad two", "the script returned nothing")
      m.hcursor.get[Int]("failed") shouldBe Right(2)
    }
  }

  "flatMap needs a list" in {
    run("return 5;", "flatMap", json("""{"a": 1}""")).flatMap { case (out, h) => h.dead.get.map { d => out shouldBe Nil; d.size shouldBe 1 } }
  }

  "an element whose script runs for ever is a dead letter, and the next one is handled" in {
    val limits = ScriptSandbox.Limits(timeout = 300.millis, statements = Long.MaxValue / 2)
    runOperator(op(limits), StreamScript.Kind, params("if (element.n === 1) { while (true) {} } return element.n;", "map"), plain(json("""{"n": 1}"""), json("""{"n": 2}"""))).flatMap { case (out, h) =>
      h.dead.get.map { dead =>
        out shouldBe numbers(2)
        dead.head.hcursor.get[String]("error").toOption.get should include("too long")
      }
    }
  }

  "the code is compiled when the flow is saved" in {
    StreamScript.descriptor.checkParams(params("return element;", "map")) shouldBe Nil
    StreamScript.descriptor.checkParams(params("return = ;", "map")).head should startWith("code: line 1:")
    StreamScript.descriptor.checkParams(params("return 1;", "sideways")).head should include("mode")
  }

  "bad syntax makes the whole segment fail when it starts" in {
    runOperator(op(), StreamScript.Kind, params("return = ;", "map"), plain(json("1"))).attempt.map(_.isLeft shouldBe true)
  }

  "it is a stream operator with one input and one output, and the catalog has both script nodes" in {
    StreamScript.descriptor.isStream(Json.obj()) shouldBe true
    StreamScript.descriptor.inputs(Json.obj()).map(_.name) shouldBe List(Port.In)
    ScriptNodes.catalog.all.map(_.kind) shouldBe List("script.js", "stream.script")
  }
}
