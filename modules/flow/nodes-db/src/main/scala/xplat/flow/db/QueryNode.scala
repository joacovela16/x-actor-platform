package xplat.flow.db

import cats.effect.Async
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Reads rows with a parameterised `SELECT` (or `WITH`, `SHOW`...). Writes nothing: the statement runs on a connection that is read only,
  * and one that is not a read is refused when the flow is saved. The rows come as a list of objects, in the outputs it declares:
  *
  *  - `rows`: `list[{}]`, at most `maxRows` (the audit says if there were more);
  *  - `count`: how many rows came;
  *  - `first`: the first row, or `null`.
  *
  * Column types become JSON as the database says: booleans and numbers as such (decimals keep their digits), text as text,
  * dates and times as ISO text, binary as base64, `json` and `jsonb` parsed. Two columns with the same name: the last one stays.
  * Use `?` for the parameters, one for each entry of `params`: `SELECT * FROM users WHERE id = ? AND active = ?`.
  */
final class QueryNode[F[_]](connections: Connections[F], pool: DbPool[F])(implicit F: Async[F]) extends DbNode[F](connections, pool) {

  val descriptor: NodeDescriptor = QueryNode.descriptor

  def plan(input: NodeInput): Either[String, DbNode.Plan] =
    for {
      p <- QueryNode.params(input.params)
      values <- ParamSpec.resolveAll(p.params, input)
    } yield DbNode.Plan(p.connection, p.sql, values, DbNode.Access.Read, p.maxRows, p.timeoutSeconds, s"db.query on '${p.connection}'")
}

object QueryNode {

  val Kind: String = "db.query"

  final case class Params(connection: String, sql: String, params: List[ParamSpec], maxRows: Int, timeoutSeconds: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- DbNode.connection(c)
      sql <- c.get[String]("sql").left.map(_ => "missing field 'sql'")
      specs <- DbNode.specs(c, "params")
      maxRows <- c.getOrElse[Int]("maxRows")(DbNode.DefaultMaxRows).left.map(_.message).filterOrElse(n => n > 0 && n <= DbNode.MaxRowsLimit, s"maxRows must be between 1 and ${DbNode.MaxRowsLimit}")
      timeout <- DbNode.timeout(c)
    } yield Params(connection, sql, specs, maxRows, timeout)
  }

  private def check(json: Json): List[String] =
    params(json) match {
      case Left(problem) => List(problem)
      case Right(p) =>
        List(
          Option.when(Sql.placeholders(p.sql) != p.params.size)(s"the statement has ${Sql.placeholders(p.sql)} '?' and params has ${p.params.size} values"),
          Option.when(!Sql.singleStatement(p.sql))("one statement only: a ';' in the middle is not allowed"),
          Option.when(!Sql.looksLikeRead(p.sql))("db.query only reads (SELECT, WITH, SHOW, EXPLAIN, VALUES); use db.execute, db.insert, db.update or db.delete to write")
        ).flatten
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Query",
    description = "Reads rows with an SQL SELECT and writes them in the state as a list of objects (rows, count, first).",
    config = ConfigSchema(
      List(
        DbNode.connectionField,
        ConfigSchema.Field("sql", DataType.Str, description = Some("The statement, with ? for each parameter."), ui = Some(UiSpec.code("sql", 8))),
        ConfigSchema.Field("params", DataType.AnyJson, required = false, default = Some(Json.arr()), description = Some("One for each ?: {\"path\": \"state.path\"}, {\"value\": 1} or {\"context\": \"key\"}")),
        ConfigSchema.Field("maxRows", DataType.Num, required = false, default = Some(Json.fromInt(DbNode.DefaultMaxRows)), description = Some("Most rows kept")),
        DbNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Read,
    defaultOutputs = _ => List(DbNode.rowsOutput),
    reads = json => params(json).toOption.toList.flatMap(p => DbNode.reads(p.params)),
    checkParams = check
  )
}
