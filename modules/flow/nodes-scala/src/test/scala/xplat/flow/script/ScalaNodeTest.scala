package xplat.flow.script

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model.{NodeInput, Port}

class ScalaNodeTest extends AnyFreeSpec with Matchers {

  private val node = new ScalaNode[IO]

  private def exec(code: String, state: Json = Json.obj(), extra: List[(String, Json)] = Nil) =
    node.run(NodeInput(Json.obj(("code" -> Json.fromString(code)) :: extra: _*), state)).unsafeRunSync()

  "runs Scala written as the body of a function, with circe and the helpers" in {
    val code =
      """val items = state.hcursor.downField("order").downField("items").as[List[Json]].getOrElse(Nil)
        |val total = items.flatMap(i => i.hcursor.get[Double]("price").toOption).sum
        |log(s"${items.size} items")
        |out("count" -> items.size.asJson, "total" -> total.asJson)""".stripMargin
    val state = Json.obj("order" -> Json.obj("items" -> Json.arr(Json.obj("price" -> Json.fromInt(10)), Json.obj("price" -> Json.fromDoubleOrNull(2.5)))))
    val o = exec(code, state)
    o.error shouldBe None
    o.values shouldBe Map("count" -> Json.fromInt(2), "total" -> Json.fromDoubleOrNull(12.5))
    o.info("logs") shouldBe Json.arr(Json.fromString("2 items"))
  }

  "chooses a port, and sees its args as params" in {
    val o = exec("""via(if (params.hcursor.get[Int]("n").contains(1)) "one" else "other")""", Json.obj(), List("ports" -> Json.arr(Json.fromString("one"), Json.fromString("other")), "args" -> Json.obj("n" -> Json.fromInt(1))))
    o.port shouldBe "one"
  }

  "code that does not compile fails with the line as the user wrote it" in {
    val o = exec("val a = 1\nval b: String = a\nout()")
    o.port shouldBe Port.Error
    o.error.get should include("line 2")
  }

  "an exception fails the node with its line" in {
    val o = exec("val x = 1\nthrow new IllegalStateException(\"boom\")")
    o.error.get should (include("boom") and include("line 2"))
  }

  private def files(fs: (String, String)*): (String, Json) =
    "files" -> Json.arr(fs.map { case (n, c) => Json.obj("name" -> Json.fromString(n), "code" -> Json.fromString(c)) }: _*)

  "the files are compiled with the code: what they define, the code uses" in {
    val helpers = files(
      "model.scala" -> "final case class Line(price: Double, qty: Int) {\n  def total: Double = price * qty\n}",
      "helpers.scala" -> "def lines(j: Json): List[Line] =\n  j.hcursor.get[List[Json]](\"lines\").getOrElse(Nil).map(l => Line(l.hcursor.get[Double](\"price\").getOrElse(0d), l.hcursor.get[Int](\"qty\").getOrElse(0)))"
    )
    val state = Json.obj("lines" -> Json.arr(Json.obj("price" -> Json.fromInt(5), "qty" -> Json.fromInt(3))))
    val o = exec("out(\"total\" -> lines(state).map(_.total).sum.asJson)", state, List(helpers))
    o.error shouldBe None
    o.values shouldBe Map("total" -> Json.fromDoubleOrNull(15.0))
  }

  "errors in a file say the file and its line, when compiling and when running" in {
    val bad = exec("out()", extra = List(files("helpers.scala" -> "val a = 1\nval b: String = a")))
    bad.error.get should include("helpers.scala line 2")
    val thrown = exec("out(\"x\" -> boom().asJson)", extra = List(files("helpers.scala" -> "def boom(): Int =\n  throw new IllegalStateException(\"boom\")")))
    thrown.error.get should (include("boom") and include("helpers.scala line 2"))
  }

  "the names of the files are checked" in {
    ScalaNode.descriptor.checkParams(Json.obj("code" -> Json.fromString("out()"), files("x.js" -> ""))).size shouldBe 1
  }

  "the template of a new node compiles and writes its default outputs" in {
    val state = Json.obj("order" -> Json.obj("items" -> Json.arr(Json.obj("price" -> Json.fromInt(600), "qty" -> Json.fromInt(2)))))
    val o = exec(ScalaNode.Template, state)
    o.error shouldBe None
    o.values.keySet shouldBe ScriptNode.TemplateOutputs.map(_.name).toSet
    o.values("big") shouldBe Json.True
  }
}
