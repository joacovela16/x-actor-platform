package xplat.flow.engine

import xplat.flow.model.CronExpression
import xplat.flow.nodes.CronNode

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit
import scala.util.Try

/** The calendar side of cron: when an expression matches, in a time zone, using `java.time`.
  *
  * Times that do not exist, because the clock jumps forward, are run at the first instant after the jump. Times that happen
  * twice, because the clock goes back, are run once, at the first of them.
  */
object CronSchedule {

  /** What the nodes and schedulers need from the clock: see [[CronNode.NextFire]]. */
  val nextFire: CronNode.NextFire = (expression, zoneId, after) =>
    zone(zoneId).flatMap { z =>
      next(expression, z, Instant.ofEpochMilli(after)).map(_.toEpochMilli).toRight(s"'${expression.text}' never matches")
    }

  def zone(id: String): Either[String, ZoneId] =
    Try(ZoneId.of(id)).toEither.left.map(_ => s"'$id' is not a time zone")

  /** How far ahead a match is looked for: a rule such as the 29th of February on a Monday comes within 28 years. */
  private val Horizon = 366 * 30

  /** The first time the expression matches strictly after `after`, if it ever does. */
  def next(expression: CronExpression, zone: ZoneId, after: Instant): Option[Instant] = {
    @scala.annotation.tailrec
    def search(from: LocalDateTime, guard: Int): Option[Instant] =
      if (guard <= 0) None
      else
        nextLocal(expression, from) match {
          case None => None
          case Some(local) =>
            val instant = ZonedDateTime.ofLocal(local, zone, null).toInstant
            // A time at the end of a jump could resolve to one that is not after `after`: go on from it.
            if (instant.isAfter(after)) Some(instant) else search(local, guard - 1)
        }
    search(LocalDateTime.ofInstant(after, zone).truncatedTo(ChronoUnit.SECONDS), 8)
  }

  /** The local date and time, strictly after `from`, that the expression matches. */
  private def nextLocal(expression: CronExpression, from: LocalDateTime): Option[LocalDateTime] = {
    @scala.annotation.tailrec
    def day(date: LocalDate, after: Option[LocalTime], left: Int): Option[LocalDateTime] =
      if (left <= 0) None
      else {
        val weekday = date.getDayOfWeek.getValue % 7 // Sunday is 7 in java.time and 0 in cron
        val found =
          if (expression.matchesDay(date.getDayOfMonth, date.getMonthValue, weekday)) timeAfter(expression, after) else None
        found match {
          case Some(time) => Some(LocalDateTime.of(date, time))
          case None       => day(date.plusDays(1), None, left - 1)
        }
      }
    day(from.toLocalDate, Some(from.toLocalTime), Horizon)
  }

  /** The first time of the day, strictly after `after` when there is one, that the hours, minutes and seconds allow. */
  private def timeAfter(expression: CronExpression, after: Option[LocalTime]): Option[LocalTime] = {
    val candidates = for {
      h <- expression.hours.iterator
      m <- expression.minutes.iterator
      s <- expression.seconds.iterator
    } yield LocalTime.of(h, m, s)
    candidates.find(t => after.forall(t.isAfter))
  }

  /** The times in `(from, to]` that the expression matches, in order, at most `limit` of them. */
  def ticks(expression: CronExpression, zone: ZoneId, from: Instant, to: Instant, limit: Int): List[Instant] = {
    @scala.annotation.tailrec
    def loop(after: Instant, acc: List[Instant]): List[Instant] =
      if (acc.size >= limit) acc
      else
        next(expression, zone, after) match {
          case Some(t) if !t.isAfter(to) => loop(t, t :: acc)
          case _                         => acc
        }
    loop(from, Nil).reverse
  }
}
