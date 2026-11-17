package xplat.flow.model

/** A cron expression, read and checked, without any notion of time zones or of the calendar of a year: that is for whoever
  * computes the next time it matches (the engine, with `java.time`). The syntax is the usual one:
  *
  *   - 5 fields, `minute hour day-of-month month day-of-week`, or 6 with `second` first (5 fields mean second 0).
  *   - In each field: `*` (or `?`), a number, a range `a-b`, a list `a,b,c`, and steps (a star, a range or a number, then a slash and the step: every 15 minutes is a star, a slash and 15).
  *   - Months may be `JAN`..`DEC` and days of the week `SUN`..`SAT` (any case); day of the week is 0-7 with 0 and 7 both Sunday.
  *   - `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`.
  *   - When both day-of-month and day-of-week are restricted (neither starts with `*`) a day matches if either does, as in cron.
  *
  * Every set is sorted, so the next value after a given one is a search in a small vector.
  *
  * @param daysOfWeek
  *   0 to 6, with 0 for Sunday.
  */
final case class CronExpression(
    text: String,
    seconds: Vector[Int],
    minutes: Vector[Int],
    hours: Vector[Int],
    daysOfMonth: Vector[Int],
    months: Vector[Int],
    daysOfWeek: Vector[Int],
    anyDayOfMonth: Boolean,
    anyDayOfWeek: Boolean
) {

  /** Whether a day matches the month, day of the month and day of the week (1-12, 1-31, 0-6 with 0 Sunday). */
  def matchesDay(dayOfMonth: Int, month: Int, dayOfWeek: Int): Boolean =
    months.contains(month) && {
      val byDate = daysOfMonth.contains(dayOfMonth)
      val byWeekday = daysOfWeek.contains(dayOfWeek)
      if (anyDayOfMonth && anyDayOfWeek) true
      else if (anyDayOfMonth) byWeekday
      else if (anyDayOfWeek) byDate
      else byDate || byWeekday
    }

  override def toString: String = text
}

object CronExpression {

  private val Shortcuts: Map[String, String] = Map(
    "@yearly" -> "0 0 1 1 *",
    "@annually" -> "0 0 1 1 *",
    "@monthly" -> "0 0 1 * *",
    "@weekly" -> "0 0 * * 0",
    "@daily" -> "0 0 * * *",
    "@midnight" -> "0 0 * * *",
    "@hourly" -> "0 * * * *"
  )

  private val MonthNames: Map[String, Int] =
    List("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC").zipWithIndex.map { case (n, i) => n -> (i + 1) }.toMap

  private val DayNames: Map[String, Int] =
    List("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT").zipWithIndex.toMap

  /** What each field accepts. */
  private final case class Spec(name: String, min: Int, max: Int, names: Map[String, Int] = Map.empty)

  private val Second = Spec("second", 0, 59)
  private val Minute = Spec("minute", 0, 59)
  private val Hour = Spec("hour", 0, 23)
  private val DayOfMonth = Spec("day of month", 1, 31)
  private val Month = Spec("month", 1, 12, MonthNames)
  private val DayOfWeek = Spec("day of week", 0, 7, DayNames)

  def parse(input: String): Either[String, CronExpression] = {
    val text = input.trim
    val expanded = Shortcuts.getOrElse(text.toLowerCase, text)
    if (text.startsWith("@") && !Shortcuts.contains(text.toLowerCase))
      Left(s"unknown shortcut '$text'; use one of ${Shortcuts.keys.toList.sorted.mkString(", ")}")
    else {
      val parts = expanded.split("\\s+").toList.filter(_.nonEmpty)
      val six = parts.size match {
        case 5     => Right("0" :: parts)
        case 6     => Right(parts)
        case other => Left(s"a cron expression has 5 or 6 fields, this one has $other")
      }
      six.flatMap {
        case List(s, mi, h, dom, mo, dow) =>
          for {
            seconds <- field(s, Second)
            minutes <- field(mi, Minute)
            hours <- field(h, Hour)
            days <- field(dom, DayOfMonth)
            months <- field(mo, Month)
            weekdays <- field(dow, DayOfWeek)
          } yield CronExpression(
            text,
            seconds,
            minutes,
            hours,
            days,
            months,
            weekdays.map(_ % 7).distinct.sorted,
            anyDayOfMonth = isAny(dom),
            anyDayOfWeek = isAny(dow)
          )
        case _ => Left("a cron expression has 5 or 6 fields")
      }
    }
  }

  private def isAny(field: String): Boolean = field.startsWith("*") || field.startsWith("?")

  private def field(text: String, spec: Spec): Either[String, Vector[Int]] =
    text.split(",", -1).toList.foldLeft[Either[String, Vector[Int]]](Right(Vector.empty)) { (acc, part) =>
      for { so <- acc; values <- item(part, spec) } yield so ++ values
    }.map(_.distinct.sorted)

  private def number(text: String, spec: Spec): Either[String, Int] =
    spec.names.get(text.toUpperCase).orElse(if (text.nonEmpty && text.forall(_.isDigit) && text.length <= 4) Some(text.toInt) else None) match {
      case None => Left(s"${spec.name}: '$text' is not a number${if (spec.names.isEmpty) "" else " or a name"}")
      case Some(n) if n < spec.min || n > spec.max => Left(s"${spec.name}: $n is out of range ${spec.min}-${spec.max}")
      case Some(n) => Right(n)
    }

  private def item(text: String, spec: Spec): Either[String, Vector[Int]] = {
    val (range, stepText) = text.split("/", -1).toList match {
      case List(r)       => (r, None)
      case List(r, step) => (r, Some(step))
      case _             => (text, Some("?")) // more than one slash: reported below
    }
    val step: Either[String, Int] = stepText match {
      case None => Right(1)
      case Some(s) if s.nonEmpty && s.forall(_.isDigit) && s.length <= 4 && s.toInt > 0 => Right(s.toInt)
      case Some(s) => Left(s"${spec.name}: the step '$s' must be a positive number")
    }
    val bounds: Either[String, (Int, Int)] = range match {
      case "*" | "?" => Right((spec.min, spec.max))
      case r if r.contains("-") =>
        r.split("-", -1).toList match {
          case List(a, b) =>
            for { from <- number(a, spec); to <- number(b, spec); _ <- if (from <= to) Right(()) else Left(s"${spec.name}: the range '$r' goes backwards") } yield (from, to)
          case _ => Left(s"${spec.name}: '$r' is not a range")
        }
      // `5/10` runs from 5 on, every 10; `5` alone is just 5.
      case r => number(r, spec).map(n => if (stepText.isDefined) (n, spec.max) else (n, n))
    }
    step.flatMap(by => bounds.map(range => Vector.range(range._1, range._2 + 1, by)))
  }
}
