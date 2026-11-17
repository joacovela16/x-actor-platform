package xplat.flow.db

import cats.effect.{Async, Resource}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections
import xplat.flow.stream.{Borrow, Elem, StreamContext, StreamPorts, StreamSource}

import java.sql.{Connection, PreparedStatement, ResultSet}

/** The rows of a `SELECT`, as a stream: the driver brings them `fetchSize` at a time as the stream asks for them, so a table of any
  * size goes through without being in memory, and a slow sink holds back the reads. The statement runs in a read-only transaction
  * on one connection of the pool, which it keeps until the stream ends. `params` are as in `db.query`, read when the segment starts.
  * Unlike `db.query` there is no `maxRows` (a `stream.take` after it does that, and closes the query).
  *
  * PostgreSQL only streams inside a transaction, which is why it does not autocommit; MySQL needs `useCursorFetch=true` in the
  * connection for `fetchSize` to be honoured.
  */
final class DbSource[F[_]](connections: Connections[F], pool: DbPool[F])(implicit F: Async[F]) extends StreamSource[F] {

  val descriptor: NodeDescriptor = DbSource.descriptor

  def open(ctx: StreamContext[F]): Stream[F, Elem[F]] =
    DbSource.params(ctx.params).flatMap(p => ParamSpec.resolveAll(p.params, ctx.input).map(p -> _)) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right((p, values)) =>
        Stream.eval(connections.resolve(p.connection)).flatMap {
          case None => Stream.raiseError[F](new IllegalArgumentException(s"the connection '${p.connection}' does not exist"))
          case Some(c) =>
            val rows: Resource[F, ResultSet] =
              for {
                open <- Borrow[F, DbPool.Open[F]](f => pool.use(c)(f))
                conn <- open.transactor.connect(open.transactor.kernel)
                _ <- Resource.make(F.blocking(DbSource.begin(conn, open.kind)))(_ => F.blocking(DbSource.end(conn, open.kind)).attempt.void)
                ps <- Resource.make(F.blocking(DbSource.prepare(conn, p, values)))(ps => F.blocking(ps.close()).attempt.void)
                rs <- Resource.make(F.blocking(ps.executeQuery()))(rs => F.blocking(rs.close()).attempt.void)
              } yield rs
            Stream
              .resource(rows)
              .flatMap(rs => Stream.fromBlockingIterator[F](Sql.rowIterator(rs), p.fetchSize))
              .evalMap(row => ctx.now.map(Elem.of[F](row, _)))
              .adaptError { case e => new RuntimeException(DbNode.clean(s"stream of '${p.connection}' failed: ${Option(e.getMessage).getOrElse(e.toString)}", c.secrets), e) }
        }
    }
}

object DbSource {
  val Kind: String = "stream.source.db"

  final case class Params(connection: String, sql: String, params: List[ParamSpec], fetchSize: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- DbNode.connection(c)
      sql <- c.get[String]("sql").left.map(_ => "missing field 'sql'")
      specs <- DbNode.specs(c, "params")
      fetch <- c.getOrElse[Int]("fetchSize")(500).left.map(_.message).filterOrElse(n => n > 0 && n <= 10000, "fetchSize must be between 1 and 10000")
    } yield Params(connection, sql, specs, fetch)
  }

  private def check(json: Json): List[String] =
    params(json) match {
      case Left(problem) => List(problem)
      case Right(p) =>
        List(
          Option.when(Sql.placeholders(p.sql) != p.params.size)(s"the statement has ${Sql.placeholders(p.sql)} '?' and params has ${p.params.size} values"),
          Option.when(!Sql.singleStatement(p.sql))("one statement only: a ';' in the middle is not allowed"),
          Option.when(!Sql.looksLikeRead(p.sql))("a stream of rows only reads (SELECT, WITH, SHOW, EXPLAIN, VALUES)")
        ).flatten
    }

  private def begin(conn: Connection, kind: String): Unit = {
    conn.setAutoCommit(false)
    DbNode.readOnly(conn, kind, on = true)
  }

  private def end(conn: Connection, kind: String): Unit =
    try conn.rollback()
    finally {
      DbNode.readOnly(conn, kind, on = false)
      conn.setAutoCommit(true)
    }

  private def prepare(conn: Connection, p: Params, values: List[Json]): PreparedStatement = {
    val ps = conn.prepareStatement(p.sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    ps.setFetchSize(p.fetchSize)
    Sql.bind(ps, values)
    ps
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SourceCategory,
    label = "SQL rows",
    description = "Starts a stream with the rows of an SQL SELECT, read in batches as the stream asks for them: for tables of any size.",
    config = ConfigSchema(
      List(
        DbNode.connectionField,
        ConfigSchema.Field("sql", DataType.Str, description = Some("The statement, with ? for each parameter."), ui = Some(UiSpec.code("sql", 8))),
        ConfigSchema.Field("params", DataType.AnyJson, required = false, default = Some(Json.arr()), description = Some("One for each ?: {\"path\": \"state.path\"}, {\"value\": 1} or {\"context\": \"key\"}")),
        ConfigSchema.Field("fetchSize", DataType.Num, required = false, default = Some(Json.fromInt(500)), description = Some("Rows brought from the database at a time"))
      )
    ),
    sideEffects = SideEffects.Read,
    ports = _ => StreamPorts.output(DataType.Obj(Map.empty)),
    reads = json => params(json).toOption.toList.flatMap(p => DbNode.reads(p.params)),
    checkParams = check
  )
}
