package xplat.flow.db

import cats.effect.Async
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Runs a parameterised statement that writes (`INSERT`, `UPDATE`, `DELETE`, DDL...) and answers how many rows it changed, in the output
  * `affected`. It is for what [[InsertNode]], [[UpdateNode]] and [[DeleteNode]] cannot say, such as `INSERT ... ON CONFLICT DO NOTHING`.
  * One statement, with `?` for each entry of `params`.
  */
final class ExecuteNode[F[_]](connections: Connections[F], pool: DbPool[F])(implicit F: Async[F]) extends DbNode[F](connections, pool) {

  val descriptor: NodeDescriptor = ExecuteNode.descriptor

  def plan(input: NodeInput): Either[String, DbNode.Plan] =
    for {
      p <- ExecuteNode.params(input.params)
      values <- ParamSpec.resolveAll(p.params, input)
    } yield DbNode.Plan(p.connection, p.sql, values, DbNode.Access.Write, 0, p.timeoutSeconds, s"db.execute on '${p.connection}'")
}

object ExecuteNode {

  val Kind: String = "db.execute"

  final case class Params(connection: String, sql: String, params: List[ParamSpec], timeoutSeconds: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- DbNode.connection(c)
      sql <- c.get[String]("sql").left.map(_ => "missing field 'sql'")
      specs <- DbNode.specs(c, "params")
      timeout <- DbNode.timeout(c)
    } yield Params(connection, sql, specs, timeout)
  }

  private def check(json: Json): List[String] =
    params(json) match {
      case Left(problem) => List(problem)
      case Right(p) =>
        List(
          Option.when(Sql.placeholders(p.sql) != p.params.size)(s"the statement has ${Sql.placeholders(p.sql)} '?' and params has ${p.params.size} values"),
          Option.when(!Sql.singleStatement(p.sql))("one statement only: a ';' in the middle is not allowed"),
          Option.when(Sql.looksLikeRead(p.sql))("this statement reads: use db.query")
        ).flatten
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Execute",
    description = "Runs an SQL statement that writes (INSERT, UPDATE, DELETE, DDL...) and answers how many rows it changed.",
    config = ConfigSchema(
      List(
        DbNode.connectionField,
        ConfigSchema.Field("sql", DataType.Str, description = Some("The statement, with ? for each parameter."), ui = Some(UiSpec.code("sql", 8))),
        ConfigSchema.Field("params", DataType.AnyJson, required = false, default = Some(Json.arr()), description = Some("One for each ?: {\"path\": \"state.path\"}, {\"value\": 1} or {\"context\": \"key\"}")),
        DbNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(DbNode.affectedOutput),
    reads = json => params(json).toOption.toList.flatMap(p => DbNode.reads(p.params)),
    checkParams = check
  )
}
