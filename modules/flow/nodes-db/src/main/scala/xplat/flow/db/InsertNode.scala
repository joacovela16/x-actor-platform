package xplat.flow.db

import cats.effect.Async
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Inserts one row: `values` says, for each column, where its value comes from. The statement is
  * `INSERT INTO <table> (<columns>) VALUES (?, ...)`, built from names that were checked to be plain names, with the values as
  * parameters. It answers `affected`.
  */
final class InsertNode[F[_]](connections: Connections[F], pool: DbPool[F])(implicit F: Async[F]) extends DbNode[F](connections, pool) {

  val descriptor: NodeDescriptor = InsertNode.descriptor

  def plan(input: NodeInput): Either[String, DbNode.Plan] =
    for {
      p <- InsertNode.params(input.params)
      values <- ParamSpec.resolveAll(p.values.map(_._2), input)
    } yield DbNode.Plan(
      p.connection,
      s"INSERT INTO ${p.table} (${p.values.map(_._1).mkString(", ")}) VALUES (${p.values.map(_ => "?").mkString(", ")})",
      values,
      DbNode.Access.Write,
      0,
      p.timeoutSeconds,
      s"db.insert into ${p.table}"
    )
}

object InsertNode {

  val Kind: String = "db.insert"

  final case class Params(connection: String, table: String, values: List[(String, ParamSpec)], timeoutSeconds: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- DbNode.connection(c)
      table <- c.get[String]("table").left.map(_ => "missing field 'table'").flatMap(Sql.identifier)
      values <- DbNode.specMap(c, "values")
      _ <- Either.cond(values.nonEmpty, (), "values needs at least one column")
      _ <- values.map(_._1).foldLeft[Either[String, Unit]](Right(()))((acc, name) => acc.flatMap(_ => Sql.identifier(name).map(_ => ())))
      timeout <- DbNode.timeout(c)
    } yield Params(connection, table, values, timeout)
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Insert",
    description = "Inserts one row into a table, with values taken from the state or fixed.",
    config = ConfigSchema(
      List(
        DbNode.connectionField,
        ConfigSchema.Field("table", DataType.Str, description = Some("Table, such as users or public.users")),
        ConfigSchema.Field("values", DataType.AnyJson, description = Some("Column -> {\"path\": \"state.path\"}, {\"value\": 1} or {\"context\": \"key\"}")),
        DbNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(DbNode.affectedOutput),
    reads = json => params(json).toOption.toList.flatMap(p => DbNode.reads(p.values.map(_._2))),
    checkParams = json => params(json).left.toOption.toList
  )
}
