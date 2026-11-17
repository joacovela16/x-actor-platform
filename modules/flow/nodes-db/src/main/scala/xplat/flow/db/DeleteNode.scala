package xplat.flow.db

import cats.effect.Async
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Deletes the rows where every column of `where` is equal to its value (`null` is compared with `IS NULL`). Without `where` it refuses
  * to run, unless `allowAll` is set. It answers `affected`.
  */
final class DeleteNode[F[_]](connections: Connections[F], pool: DbPool[F])(implicit F: Async[F]) extends DbNode[F](connections, pool) {

  val descriptor: NodeDescriptor = DeleteNode.descriptor

  def plan(input: NodeInput): Either[String, DbNode.Plan] =
    for {
      p <- DeleteNode.params(input.params)
      where <- ParamSpec.resolveAll(p.where.map(_._2), input)
    } yield {
      val (conditions, whereParams) = Where.build(p.where.map(_._1).zip(where))
      DbNode.Plan(p.connection, s"DELETE FROM ${p.table}${Where.clause(conditions)}", whereParams, DbNode.Access.Write, 0, p.timeoutSeconds, s"db.delete on ${p.table}")
    }
}

object DeleteNode {

  val Kind: String = "db.delete"

  final case class Params(connection: String, table: String, where: List[(String, ParamSpec)], timeoutSeconds: Int)

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- DbNode.connection(c)
      table <- c.get[String]("table").left.map(_ => "missing field 'table'").flatMap(Sql.identifier)
      where <- DbNode.specMap(c, "where")
      allowAll <- c.getOrElse[Boolean]("allowAll")(false).left.map(_.message)
      _ <- Either.cond(where.nonEmpty || allowAll, (), "where is empty: it would delete every row; add conditions or set allowAll")
      _ <- where.map(_._1).foldLeft[Either[String, Unit]](Right(()))((acc, name) => acc.flatMap(_ => Sql.identifier(name).map(_ => ())))
      timeout <- DbNode.timeout(c)
    } yield Params(connection, table, where, timeout)
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Delete",
    description = "Deletes the rows of a table where the columns of 'where' have the given values.",
    config = ConfigSchema(
      List(
        DbNode.connectionField,
        ConfigSchema.Field("table", DataType.Str),
        ConfigSchema.Field("where", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Column -> value it must be equal to")),
        ConfigSchema.Field("allowAll", DataType.Bool, required = false, default = Some(Json.False), description = Some("Allow a delete with no where, that removes every row")),
        DbNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(DbNode.affectedOutput),
    reads = json => params(json).toOption.toList.flatMap(p => DbNode.reads(p.where.map(_._2))),
    checkParams = json => params(json).left.toOption.toList
  )
}
