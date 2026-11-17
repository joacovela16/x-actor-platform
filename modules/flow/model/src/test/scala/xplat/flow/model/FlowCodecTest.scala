package xplat.flow.model

import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class FlowCodecTest extends AnyFreeSpec with Matchers {

  private def roundTrip[A: Encoder: Decoder](value: A): Either[io.circe.Error, A] =
    parse(value.asJson.noSpaces).flatMap(_.as[A])

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)

  "data types are written with a type tag and round-trip" in {
    val tpe = DataType.Obj(Map("total" -> DataType.Num, "items" -> DataType.ListOf(DataType.Obj(Map("sku" -> DataType.Str))), "ok" -> DataType.Bool, "x" -> DataType.AnyJson))
    roundTrip[DataType](tpe) shouldBe Right(tpe)
    (DataType.Bool: DataType).asJson shouldBe json("""{"type":"bool"}""")
    (DataType.ListOf(DataType.Str): DataType).asJson shouldBe json("""{"type":"list","element":{"type":"str"}}""")
  }

  "an unknown data type is a decoding error" in {
    json("""{"type":"banana"}""").as[DataType].isLeft shouldBe true
  }

  "data types accept the values that fit" in {
    DataType.Bool.accepts(Json.True) shouldBe true
    DataType.Bool.accepts(Json.fromString("true")) shouldBe false
    DataType.ListOf(DataType.Num).accepts(json("[1,2]")) shouldBe true
    DataType.ListOf(DataType.Num).accepts(json("""[1,"a"]""")) shouldBe false
    DataType.Obj(Map("a" -> DataType.Str)).accepts(json("""{"a":"x","b":1}""")) shouldBe true
    DataType.Obj(Map("a" -> DataType.Str)).accepts(json("{}")) shouldBe false
  }

  "a type conforms to what is required of it" in {
    DataType.Bool.conformsTo(DataType.Bool) shouldBe true
    DataType.Bool.conformsTo(DataType.Str) shouldBe false
    DataType.Num.conformsTo(DataType.AnyJson) shouldBe true
    DataType.AnyJson.conformsTo(DataType.Bool) shouldBe false
    DataType.Obj(Map("a" -> DataType.Bool, "b" -> DataType.Num)).conformsTo(DataType.Obj(Map("a" -> DataType.Bool))) shouldBe true
  }

  "state paths read and write nested values" in {
    val path = StatePath.parse("order.customer.id").toOption.get
    path.get(json("""{"order":{"customer":{"id":7}}}""")) shouldBe Some(Json.fromInt(7))
    path.get(json("""{"order":{}}""")) shouldBe None
    path.set(json("""{"other":1}"""), Json.fromInt(7)) shouldBe json("""{"other":1,"order":{"customer":{"id":7}}}""")
    StatePath.parse("a..b").isLeft shouldBe true
    StatePath.parse("").isLeft shouldBe true
  }

  "a flow round-trips" in {
    val flow = FlowDef(
      id = "f1",
      name = "Flow",
      inputs = List(OutputDecl("order", StatePath(List("order")), DataType.Obj(Map("total" -> DataType.Num)))),
      nodes = List(
        NodeDef("a", "test.produce", Json.obj("k" -> Json.fromInt(1)), List(OutputDecl("ok", StatePath(List("ok")), DataType.Bool)), RunPolicy.OnceSucceeded, JoinPolicy.AnyInput, Some("A")),
        NodeDef("b", "test.branch")
      ),
      edges = List(Edge("a", "b"), Edge("a", "b", "error"))
    )
    roundTrip(flow) shouldBe Right(flow)
  }

  "the layout of the editor round-trips, is optional and is left out when empty" in {
    val laid = FlowDef("f", "F", List(NodeDef("a", "k")), layout = Map("a" -> Position(10.5, -3)))
    roundTrip(laid) shouldBe Right(laid)
    laid.asJson shouldBe json("""{"version":1,"id":"f","name":"F","revision":1,"inputs":[],"outputs":[],"nodes":[{"id":"a","kind":"k","params":{},"outputs":[],"runPolicy":{"whenSucceeded":"run","whenFailed":"run"},"joinPolicy":"all"}],"edges":[],"layout":{"a":{"x":10.5,"y":-3}}}""")
    FlowDef("f", "F", Nil).asJson.hcursor.downField("layout").focus shouldBe None
    json("""{"id":"f","name":"F","nodes":[]}""").as[FlowDef].map(_.layout) shouldBe Right(Map.empty)
  }

  "fields with a default may be missing in the JSON" in {
    val flow = json("""{"id":"f","name":"F","nodes":[{"id":"a","kind":"k"},{"id":"b","kind":"k"}],"edges":[{"from":"a","to":"b"}]}""").as[FlowDef]
    flow shouldBe Right(
      FlowDef("f", "F", List(NodeDef("a", "k"), NodeDef("b", "k")), List(Edge("a", "b")))
    )
    json("""{"whenFailed":"skip"}""").as[RunPolicy] shouldBe Right(RunPolicy(RunPolicy.Run, RunPolicy.Skip))
    json("{}").as[RunPolicy] shouldBe Right(RunPolicy.Always)
  }

  "a cycle says which nodes are in it" in {
    (FlowError.Cycle(List("b", "c")): FlowError).asJson.hcursor.get[List[String]]("nodes") shouldBe Right(List("b", "c"))
    (FlowError.Cycle(List("b", "c")): FlowError).asJson.hcursor.get[String]("message").toOption.get should include("b, c")
    (FlowError.FlowCycle(List("a", "b", "a")): FlowError).asJson.hcursor.get[List[String]]("chain") shouldBe Right(List("a", "b", "a"))
    (FlowError.UnknownKind("x", "k"): FlowError).asJson.hcursor.downField("nodes").focus shouldBe None
  }

  "run policies are written as words" in {
    RunPolicy.OnceSucceeded.asJson shouldBe json("""{"whenSucceeded":"skip","whenFailed":"run"}""")
    (JoinPolicy.AllInputs: JoinPolicy).asJson shouldBe Json.fromString("all")
    json("\"maybe\"").as[JoinPolicy].isLeft shouldBe true
  }

  "node outputs round-trip, and the error one goes through the error port" in {
    val out = NodeOutput(Map("x" -> Json.fromInt(1)), "then", Some(Json.obj("n" -> Json.fromInt(2))))
    roundTrip(out) shouldBe Right(out)
    val failed = NodeOutput.failed("boom")
    failed.port shouldBe Port.Error
    failed.failed shouldBe true
    roundTrip(failed) shouldBe Right(failed)
    json("{}").as[NodeOutput] shouldBe Right(NodeOutput())
  }

  "a config schema round-trips and validates configurations" in {
    val schema = ConfigSchema(
      List(
        ConfigSchema.Field("url", DataType.Str, description = Some("where")),
        ConfigSchema.Field("retries", DataType.Num, required = false, default = Some(Json.fromInt(3))),
        ConfigSchema.Field("mode", DataType.Str, required = false, oneOf = List(Json.fromString("a"), Json.fromString("b")), secret = true)
      )
    )
    roundTrip(schema) shouldBe Right(schema)
    schema.validate(json("""{"url":"x"}""")) shouldBe Nil
    schema.validate(json("""{"url":"x","mode":"a"}""")) shouldBe Nil
    schema.validate(json("{}")) shouldBe List("missing field 'url'")
    schema.validate(json("""{"url":1}""")) shouldBe List("field 'url' must be str")
    schema.validate(json("""{"url":"x","mode":"c"}""")) shouldBe List("""field 'mode' must be one of "a", "b"""")
    schema.validate(json("""{"url":"x","extra":1}""")) shouldBe List("unknown field 'extra'")
    schema.validate(Json.fromString("x")) shouldBe List("the configuration must be an object")
  }
}
