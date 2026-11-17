package xplat.flow.db

import java.sql.{Connection, PreparedStatement, ResultSet, Types}
import io.circe.Json

import java.time.OffsetDateTime
import java.util.Base64
import scala.util.control.NonFatal

/** The small pieces of SQL and JDBC that the nodes share. Statements are bound with parameters; the only text that is put into a
  * statement is a table or column name, and only after it was checked to be a plain name.
  */
object Sql {

  private val Identifier = """[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?""".r

  /** A table or column that can be written in a statement as it is: letters, digits, `_` and `$`, and one dot at most. */
  def identifier(name: String): Either[String, String] =
    if (Identifier.pattern.matcher(name).matches()) Right(name) else Left(s"'$name' is not a plain table or column name")

  /** The number of `?` that are parameters: those that are not in a string, a quoted name or a comment. `??` is a question mark
    * that is part of the statement (as in the `?|` operator of Postgres), not a parameter.
    */
  def placeholders(sql: String): Int = {
    var count = 0
    var i = 0
    val n = sql.length
    while (i < n) {
      sql.charAt(i) match {
        case q @ ('\'' | '"' | '`') =>
          i += 1
          var closed = false
          while (i < n && !closed) {
            if (sql.charAt(i) == q) {
              if (i + 1 < n && sql.charAt(i + 1) == q) i += 2 else { closed = true; i += 1 }
            } else i += 1
          }
        case '-' if i + 1 < n && sql.charAt(i + 1) == '-' =>
          while (i < n && sql.charAt(i) != '\n') i += 1
        case '/' if i + 1 < n && sql.charAt(i + 1) == '*' =>
          val end = sql.indexOf("*/", i + 2)
          i = if (end < 0) n else end + 2
        case '?' =>
          if (i + 1 < n && sql.charAt(i + 1) == '?') i += 2 else { count += 1; i += 1 }
        case _ => i += 1
      }
    }
    count
  }

  private val ReadKeywords = Set("select", "with", "show", "explain", "values", "describe", "pragma")

  /** The first word of the statement, in lower case, after any comments. */
  def firstWord(sql: String): String = {
    val withoutComments = sql.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("--[^\n]*", " ").trim
    withoutComments.takeWhile(c => c.isLetter).toLowerCase
  }

  /** Whether the statement starts like one that reads. Not enough to trust by itself (a `WITH` can write on Postgres): a query also
    * runs on a connection that is read only.
    */
  def looksLikeRead(sql: String): Boolean = ReadKeywords.contains(firstWord(sql))

  /** One statement only: a `;` that is not the last character is refused, so a parameterised statement cannot be followed by another. */
  def singleStatement(sql: String): Boolean = {
    val trimmed = sql.trim.stripSuffix(";").trim
    !hasSemicolonOutsideQuotes(trimmed)
  }

  private def hasSemicolonOutsideQuotes(sql: String): Boolean = {
    var i = 0
    val n = sql.length
    var found = false
    while (i < n && !found) {
      sql.charAt(i) match {
        case q @ ('\'' | '"' | '`') =>
          i += 1
          var closed = false
          while (i < n && !closed) {
            if (sql.charAt(i) == q) { if (i + 1 < n && sql.charAt(i + 1) == q) i += 2 else { closed = true; i += 1 } } else i += 1
          }
        case ';' => found = true
        case _   => i += 1
      }
    }
    found
  }

  def bind(ps: PreparedStatement, params: List[Json]): Unit =
    params.zipWithIndex.foreach { case (value, index) =>
      val i = index + 1
      value.fold(
        ps.setNull(i, Types.NULL),
        b => ps.setBoolean(i, b),
        n => n.toLong match {
          case Some(l) => ps.setLong(i, l)
          case None    => n.toBigDecimal.fold(ps.setDouble(i, n.toDouble))(bd => ps.setBigDecimal(i, bd.bigDecimal))
        },
        s => ps.setString(i, s),
        a => ps.setString(i, Json.fromValues(a).noSpaces),
        o => ps.setString(i, Json.fromJsonObject(o).noSpaces)
      )
    }

  /** What a query gave: the rows as objects, and whether there were more than `maxRows`. */
  final case class Rows(rows: Vector[Json], truncated: Boolean)

  /** Runs a statement that reads. At most `maxRows` rows are kept; one more is asked for to know whether there were others. */
  def query(connection: Connection, sql: String, params: List[Json], maxRows: Int, timeoutSeconds: Int): Rows = {
    val ps = connection.prepareStatement(sql)
    try {
      ps.setQueryTimeout(timeoutSeconds)
      ps.setMaxRows(maxRows + 1)
      bind(ps, params)
      val rs = ps.executeQuery()
      try readRows(rs, maxRows)
      finally rs.close()
    } finally ps.close()
  }

  /** Runs a statement that writes, and says how many rows it changed. */
  def update(connection: Connection, sql: String, params: List[Json], timeoutSeconds: Int): Long = {
    val ps = connection.prepareStatement(sql)
    try {
      ps.setQueryTimeout(timeoutSeconds)
      bind(ps, params)
      ps.executeLargeUpdate()
    } finally ps.close()
  }

  /** The rows of a result as objects, one at a time, as they are read: for a stream, which reads as it is asked. */
  def rowIterator(rs: ResultSet): Iterator[Json] = {
    val meta = rs.getMetaData
    val columns = (1 to meta.getColumnCount).map(i => (i, meta.getColumnLabel(i), meta.getColumnType(i), meta.getColumnTypeName(i))).toVector
    new Iterator[Json] {
      private var ready: Option[Boolean] = None
      def hasNext: Boolean = ready.getOrElse { val r = rs.next(); ready = Some(r); r }
      def next(): Json = {
        if (!hasNext) throw new NoSuchElementException("no more rows")
        ready = None
        Json.fromFields(columns.map { case (i, label, tpe, name) => label -> value(rs, i, tpe, name) })
      }
    }
  }

  private def readRows(rs: ResultSet, maxRows: Int): Rows = {
    val meta = rs.getMetaData
    val columns = (1 to meta.getColumnCount).map(i => (i, meta.getColumnLabel(i), meta.getColumnType(i), meta.getColumnTypeName(i))).toVector
    val rows = Vector.newBuilder[Json]
    var count = 0
    var more = false
    while (!more && rs.next()) {
      if (count >= maxRows) more = true
      else {
        rows += Json.fromFields(columns.map { case (i, label, tpe, name) => label -> value(rs, i, tpe, name) })
        count += 1
      }
    }
    Rows(rows.result(), more)
  }

  /** A column as JSON: booleans, whole numbers and decimals are numbers or booleans; text is text; dates and times are ISO text; binary
    * is base64; `json` and `jsonb` are parsed. A value that the driver cannot give in its type comes as text.
    */
  private def value(rs: ResultSet, i: Int, tpe: Int, typeName: String): Json = {
    def orNull[A](a: A)(f: A => Json): Json = if (rs.wasNull() || a == null) Json.Null else f(a)
    def text: Json = Option(rs.getString(i)).fold(Json.Null)(s => Json.fromString(s))
    try tpe match {
      case Types.BOOLEAN | Types.BIT                                           => orNull(rs.getBoolean(i))(Json.fromBoolean)
      case Types.TINYINT | Types.SMALLINT | Types.INTEGER | Types.BIGINT       => orNull(rs.getLong(i))(Json.fromLong)
      case Types.FLOAT | Types.REAL | Types.DOUBLE                             => orNull(rs.getDouble(i))(d => Json.fromDouble(d).getOrElse(Json.Null))
      case Types.NUMERIC | Types.DECIMAL                                       => orNull(rs.getBigDecimal(i))(bd => Json.fromBigDecimal(bd))
      case Types.DATE                                                          => orNull(rs.getDate(i))(d => Json.fromString(d.toLocalDate.toString))
      case Types.TIME                                                          => orNull(rs.getTime(i))(t => Json.fromString(t.toLocalTime.toString))
      case Types.TIMESTAMP                                                     => orNull(rs.getTimestamp(i))(t => Json.fromString(t.toLocalDateTime.toString))
      case Types.TIMESTAMP_WITH_TIMEZONE                                       => orNull(rs.getObject(i, classOf[OffsetDateTime]))(t => Json.fromString(t.toString))
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY | Types.BLOB   => orNull(rs.getBytes(i))(b => Json.fromString(Base64.getEncoder.encodeToString(b)))
      case _ if typeName.equalsIgnoreCase("json") || typeName.equalsIgnoreCase("jsonb") =>
        Option(rs.getString(i)).fold(Json.Null)(s => io.circe.parser.parse(s).getOrElse(Json.fromString(s)))
      case _ => text
    } catch {
      case NonFatal(_) => try text catch { case NonFatal(_) => Json.Null }
    }
  }
}
