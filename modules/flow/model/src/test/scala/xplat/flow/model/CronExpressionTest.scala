package xplat.flow.model

import io.circe.Json
import io.circe.syntax._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CronExpressionTest extends AnyFreeSpec with Matchers {

  private def parsed(text: String): CronExpression = CronExpression.parse(text).fold(e => fail(e), identity)
  private def problem(text: String): String = CronExpression.parse(text).left.getOrElse(fail(s"'$text' should not be valid"))

  "five fields mean second 0, six fields have the seconds first" in {
    val five = parsed("30 8 * * *")
    five.seconds shouldBe Vector(0)
    five.minutes shouldBe Vector(30)
    five.hours shouldBe Vector(8)
    five.daysOfMonth shouldBe (1 to 31).toVector
    five.months shouldBe (1 to 12).toVector
    five.daysOfWeek shouldBe (0 to 6).toVector
    val six = parsed("15 30 8 * * *")
    six.seconds shouldBe Vector(15)
    six.minutes shouldBe Vector(30)
  }

  "lists, ranges and steps" in {
    parsed("0,15,30,45 * * * *").minutes shouldBe Vector(0, 15, 30, 45)
    parsed("* 9-17 * * *").hours shouldBe (9 to 17).toVector
    parsed("*/15 * * * *").minutes shouldBe Vector(0, 15, 30, 45)
    parsed("10-40/10 * * * *").minutes shouldBe Vector(10, 20, 30, 40)
    parsed("5/20 * * * *").minutes shouldBe Vector(5, 25, 45)
    parsed("1,1,2 * * * *").minutes shouldBe Vector(1, 2)
    parsed("0 0 */10 * *").daysOfMonth shouldBe Vector(1, 11, 21, 31)
    parsed("*/20 * * * * *").seconds shouldBe Vector(0, 20, 40)
    parsed("0 12 ? * *").daysOfMonth should have size 31
  }

  "names of months and days, in any case" in {
    parsed("0 0 1 jan,Jul *").months shouldBe Vector(1, 7)
    parsed("0 0 * MAR-MAY *").months shouldBe Vector(3, 4, 5)
    parsed("0 0 * * mon-fri").daysOfWeek shouldBe Vector(1, 2, 3, 4, 5)
    parsed("0 0 * * SUN").daysOfWeek shouldBe Vector(0)
  }

  "0 and 7 are both Sunday" in {
    parsed("0 0 * * 7").daysOfWeek shouldBe Vector(0)
    parsed("0 0 * * 0,7").daysOfWeek shouldBe Vector(0)
    parsed("0 0 * * 5-7").daysOfWeek shouldBe Vector(0, 5, 6)
  }

  "shortcuts" in {
    parsed("@hourly").minutes shouldBe Vector(0)
    parsed("@hourly").hours should have size 24
    parsed("@daily") shouldBe parsed("@midnight").copy(text = "@daily")
    parsed("@daily").hours shouldBe Vector(0)
    parsed("@weekly").daysOfWeek shouldBe Vector(0)
    parsed("@monthly").daysOfMonth shouldBe Vector(1)
    parsed("@yearly").months shouldBe Vector(1)
    parsed("@ANNUALLY").months shouldBe Vector(1)
    problem("@sometimes") should include("unknown shortcut")
  }

  "whitespace does not matter" in {
    parsed("  0   12  * *   1  ").hours shouldBe Vector(12)
  }

  "mistakes are reported with the field they are in" in {
    problem("* * * *") should include("5 or 6 fields, this one has 4")
    problem("* * * * * * *") should include("has 7")
    problem("") should include("has 0")
    problem("60 * * * *") shouldBe "minute: 60 is out of range 0-59"
    problem("* 24 * * *") shouldBe "hour: 24 is out of range 0-23"
    problem("* * 0 * *") shouldBe "day of month: 0 is out of range 1-31"
    problem("* * * 13 *") shouldBe "month: 13 is out of range 1-12"
    problem("* * * * 8") shouldBe "day of week: 8 is out of range 0-7"
    problem("60 0 0 * * *") shouldBe "second: 60 is out of range 0-59"
    problem("a * * * *") should include("'a' is not a number")
    problem("* * * foo *") should include("not a number or a name")
    problem("5-1 * * * *") should include("goes backwards")
    problem("*/0 * * * *") should include("step")
    problem("*/x * * * *") should include("step")
    problem("1/2/3 * * * *") should include("step")
    problem("1-2-3 * * * *") should include("not a range")
    problem("1,,2 * * * *") should include("not a number")
    problem("-5 * * * *") should include("not a number")
    problem("99999 * * * *") should include("not a number")
  }

  "a day matches by date and weekday as cron does" in {
    val onlyWeekday = parsed("0 0 * * 1") // Mondays
    onlyWeekday.matchesDay(dayOfMonth = 10, month = 6, dayOfWeek = 1) shouldBe true
    onlyWeekday.matchesDay(10, 6, 2) shouldBe false
    val onlyDate = parsed("0 0 15 * *")
    onlyDate.matchesDay(15, 6, 3) shouldBe true
    onlyDate.matchesDay(16, 6, 3) shouldBe false
    // Both restricted: either is enough.
    val either = parsed("0 0 15 * 5")
    either.matchesDay(15, 6, 2) shouldBe true
    either.matchesDay(20, 6, 5) shouldBe true
    either.matchesDay(20, 6, 2) shouldBe false
    // Months always have to match.
    parsed("0 0 * 12 *").matchesDay(1, 11, 0) shouldBe false
    parsed("0 0 * 12 *").matchesDay(1, 12, 0) shouldBe true
    // `*/2` counts as a star.
    parsed("0 0 */2 * 1").matchesDay(2, 1, 1) shouldBe true // the weekday only
  }

  "a node output can be held until a time, and the JSON says so" in {
    val out = NodeOutput.waitUntil(1700000000000L, Map("fireAt" -> Json.fromLong(1700000000000L)))
    out.suspendUntil shouldBe Some(1700000000000L)
    out.failed shouldBe false
    io.circe.parser.parse(out.asJson.noSpaces).flatMap(_.as[NodeOutput]) shouldBe Right(out)
    NodeOutput().suspendUntil shouldBe None
    NodeOutput().asJson.hcursor.downField("suspendUntil").focus shouldBe None
    io.circe.parser.parse("{}").flatMap(_.as[NodeOutput]) shouldBe Right(NodeOutput())
  }

  "the node input carries the time and whether it is where the execution starts" in {
    val in = NodeInput(Json.obj(), Json.obj(), now = 42L, entry = true)
    io.circe.parser.parse(in.asJson.noSpaces).flatMap(_.as[NodeInput]) shouldBe Right(in)
    io.circe.parser.parse("""{"params":{},"state":{}}""").flatMap(_.as[NodeInput]).map(i => (i.now, i.entry)) shouldBe Right((0L, false))
  }
}
