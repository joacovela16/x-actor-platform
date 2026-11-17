package xplat.flow.model

import io.circe.Json
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

class UiSpecTest extends AnyFunSuite {

  test("a field with a ui spec round-trips and without it keeps the old JSON") {
    val withUi = ConfigSchema.Field("sql", DataType.Str, ui = Some(UiSpec.code("sql", 8)))
    val json = withUi.asJson
    assert(json.hcursor.downField("ui").get[String]("widget") == Right("code"))
    assert(json.hcursor.downField("ui").get[String]("language") == Right("sql"))
    assert(json.as[ConfigSchema.Field] == Right(withUi))

    val plain = ConfigSchema.Field("n", DataType.Num)
    assert(plain.asJson.hcursor.downField("ui").succeeded == false)
    assert(plain.asJson.as[ConfigSchema.Field] == Right(plain))
  }

  test("options and visibleWhen are kept") {
    val ui = UiSpec(
      widget = Some("select"),
      options = List(UiSpec.Choice(Json.fromString("a"), "A")),
      visibleWhen = Some(UiSpec.VisibleWhen("kind", Json.fromString("x")))
    )
    assert(ui.asJson.as[UiSpec] == Right(ui))
  }
}
