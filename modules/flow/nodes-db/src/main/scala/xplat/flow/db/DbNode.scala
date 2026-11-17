package xplat.flow.db

import cats.effect.Async
import cats.syntax.all._
import doobie.implicits._
import doobie.free.{connection => FC}
import io.circe.{ACursor, Json}
import xplat.flow.model._
import xplat.flow.nodes.{Connections, NodeType}

import java.sql.Connection

/** What the nodes of the database have in common: they work out one parameterised statement from the configuration and the state,
  * run it in one transaction on the pool of the named connection, and answer the values that the node declared.
  *
  * A node that reads (`db.query`) runs on a connection that is read only (for SQLite, with `query_only`). One that writes is
  * simulated in test mode: it does not connect, and the audit shows the statement and the parameters it would have run.
  *
  * **Retries.** A node that writes runs again if the instance is run again, unless its `RunPolicy` says to skip it once it
  * succeeded; even then, a crash between the write and the record of it can repeat the write. For a write that must not be repeated,
  * pass `{"context": "key"}` as a column (a unique `<instanceId>:<nodeId>`) with a unique constraint, and ignore the conflict.
  *
  * The password of the connection is never in the answer: messages of the driver are cleaned of the secrets before they are shown.
  */
abstract class DbNode[F[_]](connections: Connections[F], pool: DbPool[F])(implicit F: Async[F]) extends NodeType[F] {

  /** The statement to run for this input, or why the configuration cannot make one. */
  def plan(input: NodeInput): Either[String, DbNode.Plan]

  def run(input: NodeInput): F[NodeOutput] =
    plan(input) match {
      case Left(problem) => NodeOutput.failed(problem).pure[F]
      case Right(p) =>
        connections.resolve(p.connection).flatMap {
          case None => NodeOutput.failed(s"the connection '${p.connection}' does not exist").pure[F]
          case Some(c) =>
            val started = System.nanoTime()
            pool
              .use(c)(open => DbNode.work(p, open.kind).transact(open.transactor))
              .map(outcome => DbNode.answer(p, input, outcome, (System.nanoTime() - started) / 1000000))
              .handleError(e => NodeOutput.failed(DbNode.clean(s"${p.describe} failed: ${Option(e.getMessage).getOrElse(e.toString)}", c.secrets)))
        }
    }

  override def simulated(input: NodeInput): Option[NodeOutput] =
    Some(plan(input) match {
      case Left(problem) => NodeOutput.failed(problem)
      case Right(p) =>
        val empty = if (p.access == DbNode.Access.Read) DbNode.Outcome.Read(Sql.Rows(Vector.empty, truncated = false)) else DbNode.Outcome.Write(0L)
        DbNode.answer(p, input, empty, 0L).copy(info = DbNode.info(p, withParams = true) + ("simulated" -> Json.True))
    })
}

object DbNode {

  sealed trait Access
  object Access {
    case object Read extends Access
    case object Write extends Access
  }

  /** @param describe
    *   what is run, for messages: `db.query on 'main'`.
    */
  final case class Plan(connection: String, sql: String, params: List[Json], access: Access, maxRows: Int, timeoutSeconds: Int, describe: String)

  sealed trait Outcome
  object Outcome {
    final case class Read(rows: Sql.Rows) extends Outcome
    final case class Write(affected: Long) extends Outcome
  }

  val DefaultMaxRows: Int = 1000
  val MaxRowsLimit: Int = 100000
  val DefaultTimeoutSeconds: Int = 30

  /** The statement, in one transaction. A read goes through a connection that cannot write. */
  private def work(p: Plan, kind: String): doobie.ConnectionIO[Outcome] =
    FC.raw[Outcome] { (connection: Connection) =>
      p.access match {
        case Access.Write => Outcome.Write(Sql.update(connection, p.sql, p.params, p.timeoutSeconds))
        case Access.Read =>
          readOnly(connection, kind, on = true)
          try Outcome.Read(Sql.query(connection, p.sql, p.params, p.maxRows, p.timeoutSeconds))
          finally readOnly(connection, kind, on = false)
      }
    }

  def readOnly(connection: Connection, kind: String, on: Boolean): Unit =
    if (kind == DbKinds.Sqlite) {
      // The JDBC flag cannot change on an open SQLite connection; `query_only` does the same.
      val st = connection.createStatement()
      try st.execute(s"PRAGMA query_only = ${if (on) 1 else 0}") finally st.close()
    } else connection.setReadOnly(on)

  /** What the node answers: only the values it declared, which is what the engine accepts. A node that declares nothing (it is run on
    * its own) answers all of them.
    */
  private def answer(p: Plan, input: NodeInput, outcome: Outcome, millis: Long): NodeOutput = {
    val all: Map[String, Json] = outcome match {
      case Outcome.Read(r) =>
        Map("rows" -> Json.fromValues(r.rows), "count" -> Json.fromInt(r.rows.size), "first" -> r.rows.headOption.getOrElse(Json.Null))
      case Outcome.Write(n) => Map("affected" -> Json.fromLong(n))
    }
    val wanted = input.outputs.map(_.name)
    val unknown = wanted.filterNot(all.contains)
    if (unknown.nonEmpty) NodeOutput.failed(s"${p.describe} cannot answer ${unknown.map(n => s"'$n'").mkString(", ")}; it answers ${all.keys.toList.sorted.mkString(", ")}")
    else {
      val values = if (wanted.isEmpty) all.filter { case (k, _) => k != "first" } else all.filter { case (k, _) => wanted.contains(k) }
      val extra = outcome match {
        case Outcome.Read(r)  => Map("rows" -> Json.fromInt(r.rows.size), "truncated" -> Json.fromBoolean(r.truncated))
        case Outcome.Write(n) => Map("affected" -> Json.fromLong(n))
      }
      NodeOutput(values, info = info(p, withParams = false) ++ extra + ("durationMs" -> Json.fromLong(millis)))
    }
  }

  private def info(p: Plan, withParams: Boolean): Map[String, Json] =
    Map("connection" -> Json.fromString(p.connection), "statement" -> Json.fromString(p.sql), "paramCount" -> Json.fromInt(p.params.size)) ++
      (if (withParams) Map("params" -> Json.fromValues(p.params)) else Map.empty)

  /** `text` without any of the secret values of the connection. */
  private[db] def clean(text: String, secrets: Map[String, String]): String =
    secrets.values.filter(_.nonEmpty).foldLeft(text)((t, secret) => t.replace(secret, "***"))

  // ---- Reading the configuration ----

  def connection(c: ACursor): Either[String, String] = c.get[String]("connection").left.map(_ => "missing field 'connection'").filterOrElse(_.nonEmpty, "connection is empty")

  def timeout(c: ACursor): Either[String, Int] = c.getOrElse[Int]("timeoutSeconds")(DefaultTimeoutSeconds).left.map(_.message).filterOrElse(t => t > 0 && t <= 3600, "timeoutSeconds must be between 1 and 3600")

  def specs(c: ACursor, field: String): Either[String, List[ParamSpec]] = c.getOrElse[List[ParamSpec]](field)(Nil).left.map(e => s"$field: ${e.message}")

  def specMap(c: ACursor, field: String): Either[String, List[(String, ParamSpec)]] =
    c.getOrElse[Map[String, ParamSpec]](field)(Map.empty).left.map(e => s"$field: ${e.message}").map(_.toList.sortBy(_._1))

  /** The configuration fields that every database node has. */
  val connectionField: ConfigSchema.Field = ConfigSchema.Field("connection", DataType.Str, description = Some("Name of the connection (postgres, mariadb or sqlite)"))
  val timeoutField: ConfigSchema.Field =
    ConfigSchema.Field("timeoutSeconds", DataType.Num, required = false, default = Some(Json.fromInt(DefaultTimeoutSeconds)), description = Some("Time limit of the statement"))

  /** What a node reads from the state: the paths of its parameters. */
  def reads(specs: List[ParamSpec]): List[StateRead] = ParamSpec.reads(specs).map(StateRead(_, DataType.AnyJson))

  val rowsOutput: OutputDecl = OutputDecl("rows", StatePath(List("rows")), DataType.ListOf(DataType.Obj(Map.empty)))
  val affectedOutput: OutputDecl = OutputDecl("affected", StatePath(List("affected")), DataType.Num)
}
