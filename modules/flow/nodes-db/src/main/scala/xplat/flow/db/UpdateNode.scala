package xplat.flow.db

import cats.effect.Async
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Updates the rows where every column of `where` is equal to its value (a value that is `null` is compared with `IS NULL`). `set` says
  * the new value of each column. Without `where` it refuses to run, unless `allowAll` is set: an update of the whole table has to be
  * asked for. It answers `affected`. Names of tables and columns are checked to be plain names; values are parameters.
  */
final class UpdateNode[F[_]](connections: Connections[F], pool: DbPool[F])(implicit F: Async[F]) extends DbNode[F](connections, pool) {

  val descriptor: NodeDescriptor = UpdateNode.descriptor

  def plan(input: NodeInput): Either[String, DbNode.Plan] =
    for {
      p <- UpdateNode.params(input.params)
      set <- ParamSpec.resolveAll(p.set.map(_._2), input)
      where <- ParamSpec.resolveAll(p.where.map(_._2), input)
    } yield {
      val (conditions, whereParams) = Where.build(p.where.map(_._1).zip(where))
      val sql = s"UPDATE ${p.table} SET ${p.set.map(_._1 + " = ?").mkString(", ")}${Where.clause(conditions)}"
      DbNode.Plan(p.connection, sql, set ++ whereParams, DbNode.Access.Write, 0, p.timeoutSeconds, s"db.update on ${p.table}")
    }
}

object UpdateNode {

  val Kind: String = "db.update"

  final case class Params(connection: String, table: String, set: List[(String, ParamSpec)], where: List[(String, ParamSpec)], timeoutSeconds: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- DbNode.connection(c)
      table <- c.get[String]("table").left.map(_ => "missing field 'table'").flatMap(Sql.identifier)
      set <- DbNode.specMap(c, "set")
      _ <- Either.cond(set.nonEmpty, (), "set needs at least one column")
      where <- DbNode.specMap(c, "where")
      allowAll <- c.getOrElse[Boolean]("allowAll")(false).left.map(_.message)
      _ <- Either.cond(where.nonEmpty || allowAll, (), "where is empty: it would change every row; add conditions or set allowAll")
      _ <- (set ++ where).map(_._1).foldLeft[Either[String, Unit]](Right(()))((acc, name) => acc.flatMap(_ => Sql.identifier(name).map(_ => ())))
      timeout <- DbNode.timeout(c)
    } yield Params(connection, table, set, where, timeout)
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Update",
    description = "Updates the rows of a table where the columns of 'where' have the given values.",
    config = ConfigSchema(
      List(
        DbNode.connectionField,
        ConfigSchema.Field("table", DataType.Str),
        ConfigSchema.Field("set", DataType.AnyJson, description = Some("Column -> new value: {\"path\": ...}, {\"value\": ...} or {\"context\": ...}")),
        ConfigSchema.Field("where", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Column -> value it must be equal to")),
        ConfigSchema.Field("allowAll", DataType.Bool, required = false, default = Some(Json.False), description = Some("Allow an update with no where, that changes every row")),
        DbNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(DbNode.affectedOutput),
    reads = json => params(json).toOption.toList.flatMap(p => DbNode.reads((p.set ++ p.where).map(_._2))),
    checkParams = json => params(json).left.toOption.toList
  )
}
