package xplat.flow.script

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import xplat.flow.model._
import xplat.flow.stream.StreamSpec

import scala.concurrent.duration._

class ScriptNodeTest extends StreamSpec {

  private def params(code: String, extra: (String, Json)*): Json = Json.obj(("code" -> Json.fromString(code)) +: extra: _*)

  private def run(code: String, state: String = "{}", memory: String = "{}", limits: ScriptSandbox.Limits = ScriptSandbox.Limits(), extra: Seq[(String, Json)] = Nil): IO[NodeOutput] =
    new ScriptNode[IO](limits).run(NodeInput(params(code, extra: _*), json(state), json(memory)))

  "returns values, the port and the new memory" in {
    run(
      """const total = state.items.reduce((s, i) => s + i.price * i.qty, 0);
        |return { values: { total: total, big: total > 100 }, port: "yes", memory: { runs: (memory.runs || 0) + 1 } };""".stripMargin,
      state = """{"items": [{"price": 20, "qty": 3}, {"price": 50, "qty": 1}]}""",
      memory = """{"runs": 4}""",
      extra = Seq("ports" -> Json.arr(Json.fromString("yes"), Json.fromString("no")))
    ).map { out =>
      out shouldBe NodeOutput(Map("total" -> Json.fromInt(110), "big" -> Json.True), "yes", Some(json("""{"runs": 5}""")))
    }
  }

  "returning nothing, or null, writes nothing and goes through out" in {
    for {
      a <- run("const x = 1;")
      b <- run("return null;")
    } yield {
      a shouldBe NodeOutput()
      b shouldBe NodeOutput()
    }
  }

  "the arguments of the configuration are the params, and log lines go to the info" in {
    run("""log("hello", params.n); return { values: { n: params.n + 1 } };""", extra = Seq("args" -> json("""{"n": 41}"""))).map { out =>
      out.values shouldBe Map("n" -> Json.fromInt(42))
      out.info shouldBe Map("logs" -> json("""["hello 41"]"""))
    }
  }

  "the state cannot be changed: it is frozen, and the script is strict" in {
    for {
      assign <- run("state.a = 2; return { values: { a: state.a } };", state = """{"a": 1}""")
      deep <- run("state.o.x = 2;", state = """{"o": {"x": 1}}""")
      push <- run("state.list.push(3);", state = """{"list": [1]}""")
      memoryAssign <- run("""memory.k = 1; return { memory: memory };""", memory = "{}")
    } yield {
      assign.failed shouldBe true
      assign.error.get should include("line 1")
      deep.failed shouldBe true
      push.failed shouldBe true
      // memory is frozen too: a script that wants to keep something returns a new value.
      memoryAssign.failed shouldBe true
    }
  }

  "nothing of the machine is in reach" in {
    val attempts = List(
      "java.lang.System.exit(1)",
      "Java.type('java.lang.System')",
      "Packages.java.lang.System",
      "javax.script",
      "process.env",
      "process.exit(1)",
      "require('fs')",
      "fetch('http://localhost:1/')",
      "load('/etc/passwd')",
      "console.log('x')",
      "print('x')",
      "Polyglot.eval('js', '1')",
      "new java.io.File('/etc/passwd')",
      "readFile('/etc/passwd')",
      "globalThis.Java.type('java.io.File')"
    )
    attempts
      .traverse_(code =>
        run(s"$code; return { values: { leaked: true } };").map { out =>
          withClue(s"'$code' should not work: ") {
            out.failed shouldBe true
            out.values shouldBe empty
            // It is denied for the right reason: the name is not there, or the host class is refused. Not a syntax slip of the test.
            out.error.get should (include("is not defined") or include("is not allowed"))
          }
        }
      )
      .as(succeed)
  }

  "an endless loop is stopped by the timeout, and the next run works" in {
    val limits = ScriptSandbox.Limits(timeout = 400.millis, statements = Long.MaxValue / 2)
    for {
      started <- IO.monotonic
      out <- run("while (true) {}", limits = limits)
      took <- IO.monotonic.map(_ - started)
      after <- run("return { values: { ok: true } };", limits = limits)
    } yield {
      out.failed shouldBe true
      out.error.get should include("too long")
      took should be < 3.seconds
      after.values shouldBe Map("ok" -> Json.True)
    }
  }

  "a shorter timeout in the configuration applies, but never a longer one" in {
    val limits = ScriptSandbox.Limits(timeout = 600.millis, statements = Long.MaxValue / 2)
    for {
      short <- run("while (true) {}", limits = limits, extra = Seq("timeoutMs" -> Json.fromInt(100)))
      long <- run("while (true) {}", limits = limits, extra = Seq("timeoutMs" -> Json.fromInt(60000)))
    } yield {
      short.error.get should include("too long")
      long.error.get should include("too long")
    }
  }

  "too many statements are stopped too" in {
    run("let n = 0; while (true) { n++; }", limits = ScriptSandbox.Limits(statements = 100000L)).map { out =>
      out.failed shouldBe true
      out.error.get should include("statements")
    }
  }

  "a result that is too large is refused" in {
    run("return { values: { big: 'x'.repeat(5000) } };", limits = ScriptSandbox.Limits(maxResultBytes = 1000)).map { out =>
      out.failed shouldBe true
      out.error.get should include("larger")
    }
  }

  "an error of the script goes through the error port with its message and line" in {
    run("const a = 1;\nconst b = a + 1;\nthrow new Error('boom: ' + b);").map { out =>
      out.port shouldBe Port.Error
      out.error.get shouldBe "line 3: Error: boom: 2"
    }
  }

  "calling something that does not exist reports the line" in {
    run("const a = 1;\nundefinedFunction(a);").map(_.error.get shouldBe "line 2: ReferenceError: undefinedFunction is not defined")
  }

  "a syntax error is found when the flow is saved, with its line" in {
    val problems = ScriptNode.descriptor.checkParams(params("const a = 1;\nconst = ;"))
    problems.size shouldBe 1
    problems.head should startWith("code: line 2:")
    ScriptNode.descriptor.checkParams(params("return 1;")) shouldBe Nil
    run("const = ;").map(_.port shouldBe Port.Error)
  }

  "what the script returns must be an object with the known fields and a port of the node" in {
    for {
      number <- run("return 5;")
      unknown <- run("return { value: 1 };")
      port <- run("return { port: 'elsewhere' };")
      values <- run("return { values: 3 };")
      error <- run("return { port: 'error' };")
    } yield {
      List(number, unknown, port, values).foreach(_.failed shouldBe true)
      number.error.get should include("object")
      unknown.error.get should include("unknown fields")
      port.error.get should include("not a port")
      error.port shouldBe Port.Error
    }
  }

  "nothing is shared between runs" in {
    for {
      first <- run("globalThis.leftover = 1; Object.prototype.polluted = 1; return { values: { n: 1 } };")
      second <- run("return { values: { seen: typeof globalThis.leftover, polluted: typeof ({}).polluted } };")
    } yield {
      first.failed shouldBe false
      second.values shouldBe Map("seen" -> Json.fromString("undefined"), "polluted" -> Json.fromString("undefined"))
    }
  }

  "the ports of the configuration are the ports of the node" in {
    val d = ScriptNode.descriptor
    d.portNames(params("return 1;")) shouldBe List("out", Port.Error)
    d.portNames(params("return 1;", "ports" -> json("""["yes", "no"]"""))) shouldBe List("yes", "no", Port.Error)
    d.sideEffects shouldBe SideEffects.Pure
    d.config.validate(params("return 1;")) shouldBe Nil
    d.config.validate(Json.obj()) shouldBe List("missing field 'code'")
    d.checkParams(params("return 1;", "ports" -> json("[]"))).size shouldBe 1
    d.checkParams(params("return 1;", "ports" -> json("""["a", "a"]"""))).size shouldBe 1
    d.checkParams(params("return 1;", "ports" -> json("""["error"]"""))).size shouldBe 1
  }

  "it works inside a flow, with declared outputs checked by the validator" in {
    val flow = FlowDef(
      "f", "F",
      inputs = List(OutputDecl("n", path("n"), DataType.Num)),
      nodes = List(
        NodeDef("s", ScriptNode.Kind, params("return { values: { double: state.n * 2 } };"), outputs = List(OutputDecl("double", path("double"), DataType.Num)))
      )
    )
    IO.pure(FlowValidator.validate(flow, ScriptNodes.catalog) shouldBe Nil)
  }

  private def files(fs: (String, String)*): (String, Json) =
    "files" -> Json.arr(fs.map { case (n, c) => Json.obj("name" -> Json.fromString(n), "code" -> Json.fromString(c)) }: _*)

  "the files run before the code: what they declare, the code uses" in {
    val helpers = files("math.js" -> "function double(x) { return x * 2; }\nconst BASE = 10;", "more.js" -> "const plus = (x) => double(x) + BASE;")
    run("return { values: { n: plus(state.n) } };", state = """{"n": 4}""", extra = Seq(helpers)).map { out =>
      out.error shouldBe None
      out.values shouldBe Map("n" -> Json.fromInt(18))
    }
  }

  "an error in a file says the file and its line" in {
    val helpers = files("helpers.js" -> "function boom() {\n  throw new Error('bad');\n}")
    for {
      thrown <- run("return { values: { n: boom() } };", extra = Seq(helpers))
    } yield {
      thrown.error.get should (include("helpers.js line 2") and include("bad"))
      ScriptNode.descriptor.checkParams(params("return 1;", files("broken.js" -> "const = ;"))).mkString should include("broken.js")
    }
  }

  "the names of the files are checked" in {
    val d = ScriptNode.descriptor
    d.checkParams(params("return 1;", files("helpers.js" -> ""))) shouldBe Nil
    d.checkParams(params("return 1;", files("../x.js" -> ""))).size shouldBe 1
    d.checkParams(params("return 1;", files("a.ts" -> ""))).size shouldBe 1
    IO.pure(d.checkParams(params("return 1;", files("a.js" -> "", "a.js" -> ""))).size shouldBe 1)
  }

  "the template of a new node runs as it is, writing its default outputs" in {
    run(ScriptNode.Template, state = """{"order": {"items": [{"price": 600, "qty": 2}]}}""").map { out =>
      out.error shouldBe None
      out.values shouldBe Map("total" -> Json.fromInt(1200), "big" -> Json.True)
      out.values.keySet shouldBe ScriptNode.TemplateOutputs.map(_.name).toSet
    }
  }
}
