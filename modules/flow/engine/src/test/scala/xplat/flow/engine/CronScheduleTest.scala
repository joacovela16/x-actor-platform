package xplat.flow.engine

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model.CronExpression

import java.time.{Instant, ZoneId}

class CronScheduleTest extends AnyFreeSpec with Matchers {

  private def cron(text: String): CronExpression = CronExpression.parse(text).fold(e => fail(e), identity)
  private def at(text: String): Instant = Instant.parse(text)
  private def zone(id: String): ZoneId = ZoneId.of(id)
  private def next(expression: String, zoneId: String, after: String): Option[Instant] = CronSchedule.next(cron(expression), zone(zoneId), at(after))
  private def nextIso(expression: String, zoneId: String, after: String): Option[String] = next(expression, zoneId, after).map(_.toString)

  "the next time is strictly after the given one" in {
    nextIso("*/5 * * * *", "UTC", "2024-01-01T10:02:00Z") shouldBe Some("2024-01-01T10:05:00Z")
    nextIso("*/5 * * * *", "UTC", "2024-01-01T10:05:00Z") shouldBe Some("2024-01-01T10:10:00Z")
    // Milliseconds of the starting time do not make the same second match again.
    nextIso("* * * * * *", "UTC", "2024-01-01T10:00:00.400Z") shouldBe Some("2024-01-01T10:00:01Z")
    nextIso("0 0 * * *", "UTC", "2024-01-01T23:59:59.999Z") shouldBe Some("2024-01-02T00:00:00Z")
  }

  "seconds, hours, days, months and years roll over" in {
    nextIso("30 * * * * *", "UTC", "2024-01-01T10:00:45Z") shouldBe Some("2024-01-01T10:01:30Z")
    nextIso("0 30 23 * * *", "UTC", "2024-12-31T23:30:00Z") shouldBe Some("2025-01-01T23:30:00Z")
    nextIso("0 0 1 1 *", "UTC", "2024-01-01T00:00:00Z") shouldBe Some("2025-01-01T00:00:00Z")
    nextIso("0 0 1 * *", "UTC", "2024-01-31T12:00:00Z") shouldBe Some("2024-02-01T00:00:00Z")
  }

  "days of the week: Sunday is 0 for cron" in {
    // 2024-01-01 was a Monday.
    nextIso("0 9 * * MON-FRI", "UTC", "2024-01-05T09:00:00Z") shouldBe Some("2024-01-08T09:00:00Z") // Friday -> Monday
    nextIso("0 9 * * SUN", "UTC", "2024-01-01T00:00:00Z") shouldBe Some("2024-01-07T09:00:00Z")
    nextIso("0 9 * * 0", "UTC", "2024-01-01T00:00:00Z") shouldBe Some("2024-01-07T09:00:00Z")
    nextIso("0 9 * * 7", "UTC", "2024-01-01T00:00:00Z") shouldBe Some("2024-01-07T09:00:00Z")
    nextIso("0 9 * * SAT", "UTC", "2024-01-01T00:00:00Z") shouldBe Some("2024-01-06T09:00:00Z")
  }

  "months with fewer days are skipped, and rare dates are found" in {
    nextIso("0 0 31 * *", "UTC", "2024-01-31T00:00:00Z") shouldBe Some("2024-03-31T00:00:00Z") // no 31st in February
    nextIso("0 0 29 2 *", "UTC", "2024-03-01T00:00:00Z") shouldBe Some("2028-02-29T00:00:00Z")
    nextIso("0 0 30 2 *", "UTC", "2024-01-01T00:00:00Z") shouldBe None // there is no February 30th
  }

  "day of month and day of week together: either one" in {
    // The 15th, or any Friday. 2024-03-01 was a Friday.
    nextIso("0 0 15 * 5", "UTC", "2024-02-29T12:00:00Z") shouldBe Some("2024-03-01T00:00:00Z")
    nextIso("0 0 15 * 5", "UTC", "2024-03-01T00:00:00Z") shouldBe Some("2024-03-08T00:00:00Z")
    nextIso("0 0 15 * 5", "UTC", "2024-03-14T00:00:00Z") shouldBe Some("2024-03-15T00:00:00Z")
  }

  "time zones, also with an offset that is not whole hours" in {
    // Winter in Madrid is UTC+1.
    nextIso("0 9 * * *", "Europe/Madrid", "2024-01-10T00:00:00Z") shouldBe Some("2024-01-10T08:00:00Z")
    // Summer is UTC+2.
    nextIso("0 9 * * *", "Europe/Madrid", "2024-07-10T00:00:00Z") shouldBe Some("2024-07-10T07:00:00Z")
    nextIso("0 9 * * *", "America/Argentina/Buenos_Aires", "2024-07-10T00:00:00Z") shouldBe Some("2024-07-10T12:00:00Z")
    nextIso("0 9 * * *", "Asia/Kolkata", "2024-07-10T00:00:00Z") shouldBe Some("2024-07-10T03:30:00Z")
    // A day that is not the same day in another zone.
    nextIso("0 1 * * *", "Pacific/Auckland", "2024-07-10T12:00:00Z") shouldBe Some("2024-07-10T13:00:00Z")
  }

  "when the clock goes forward, a time that does not exist runs at the first instant after the jump" in {
    // Madrid, 2024-03-31: 02:00 became 03:00 (UTC+1 to UTC+2), so 02:30 did not exist.
    nextIso("30 2 * * *", "Europe/Madrid", "2024-03-30T12:00:00Z") shouldBe Some("2024-03-31T01:30:00Z") // 03:30 +02:00
    nextIso("30 2 * * *", "Europe/Madrid", "2024-03-31T01:30:00Z") shouldBe Some("2024-04-01T00:30:00Z") // 02:30 +02:00
    // It is not run twice when 03:30 is also in the expression.
    CronSchedule.ticks(cron("30 2,3 * * *"), zone("Europe/Madrid"), at("2024-03-30T12:00:00Z"), at("2024-03-31T12:00:00Z"), 10).map(_.toString) shouldBe
      List("2024-03-31T01:30:00Z")
  }

  "when the clock goes back, a time that happens twice runs once, the first time" in {
    // Madrid, 2024-10-27: 03:00 became 02:00 (UTC+2 to UTC+1), so 02:30 happened at 00:30Z and again at 01:30Z.
    nextIso("30 2 * * *", "Europe/Madrid", "2024-10-26T12:00:00Z") shouldBe Some("2024-10-27T00:30:00Z")
    nextIso("30 2 * * *", "Europe/Madrid", "2024-10-27T00:30:00Z") shouldBe Some("2024-10-28T01:30:00Z")
    // The same goes for an expression that runs every hour: the repeated hour (02:00 and again 02:00, one real hour later) counts once,
    // so that day it runs once less. 01:00Z, which is 02:00 for the second time, has no run.
    CronSchedule.ticks(cron("0 * * * *"), zone("Europe/Madrid"), at("2024-10-26T21:59:00Z"), at("2024-10-27T02:30:00Z"), 20).map(_.toString) shouldBe
      List("2024-10-26T22:00:00Z", "2024-10-26T23:00:00Z", "2024-10-27T00:00:00Z", "2024-10-27T02:00:00Z")
  }

  "ticks: those in a range, in order, bounded by the limit" in {
    CronSchedule.ticks(cron("0 */10 * * * *"), zone("UTC"), at("2024-01-01T00:00:00Z"), at("2024-01-01T00:35:00Z"), 100).map(_.toString) shouldBe
      List("2024-01-01T00:10:00Z", "2024-01-01T00:20:00Z", "2024-01-01T00:30:00Z")
    // `from` is not included and `to` is.
    CronSchedule.ticks(cron("0 */10 * * * *"), zone("UTC"), at("2024-01-01T00:10:00Z"), at("2024-01-01T00:30:00Z"), 100).map(_.toString) shouldBe
      List("2024-01-01T00:20:00Z", "2024-01-01T00:30:00Z")
    CronSchedule.ticks(cron("* * * * * *"), zone("UTC"), at("2024-01-01T00:00:00Z"), at("2024-01-01T01:00:00Z"), 5) should have size 5
    CronSchedule.ticks(cron("@yearly"), zone("UTC"), at("2024-01-01T00:00:00Z"), at("2024-06-01T00:00:00Z"), 5) shouldBe Nil
  }

  "the function that nodes use" in {
    CronSchedule.nextFire(cron("0 9 * * *"), "Europe/Madrid", at("2024-01-10T00:00:00Z").toEpochMilli) shouldBe Right(at("2024-01-10T08:00:00Z").toEpochMilli)
    CronSchedule.nextFire(cron("0 9 * * *"), "Mars/Phobos", 0L) shouldBe Left("'Mars/Phobos' is not a time zone")
    CronSchedule.nextFire(cron("0 0 30 2 *"), "UTC", 0L) shouldBe Left("'0 0 30 2 *' never matches")
    CronSchedule.zone("UTC") shouldBe Right(ZoneId.of("UTC"))
  }
}
