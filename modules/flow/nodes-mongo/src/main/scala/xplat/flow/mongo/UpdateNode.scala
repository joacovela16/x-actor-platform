package xplat.flow.mongo

import cats.effect.Async
import io.circe.Json
import xplat.flow.nodes.Template
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Updates the first document that matches `filter` (or all of them, with `many`). `update` is a document of operators
  * (`{"$set": {...}}`) or an aggregation pipeline (a list of stages). With `upsert` a document is created when none matches. An
  * empty filter is refused unless `allowAll` is set. It answers `matched`, `affected` (the documents changed) and `upsertedId`.
  */
final class UpdateNode[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit F: Async[F]) extends MongoNode[F](connections, pool) {

  val descriptor: NodeDescriptor = UpdateNode.descriptor

  def plan(input: NodeInput): Either[String, MongoNode.Plan] =
    for {
      p <- UpdateNode.params(input.params)
      filter <- MongoNode.resolve(p.filter, input)
      update <- Template.resolve(p.update, input)
    } yield MongoNode.Plan(p.connection, MongoNode.Op.Update(p.collection, filter, update, p.many, p.upsert), p.timeoutSeconds, s"mongo.update on '${p.connection}'.${p.collection}")
}

object UpdateNode {

  val Kind: String = "mongo.update"

  final case class Params(connection: String, collection: String, filter: Json, update: Json, many: Boolean, upsert: Boolean, timeoutSeconds: Int)

  def params(raw: Json): Either[String, Params] =
    MongoNode.lenient(raw).flatMap { json =>
      val c = json.hcursor
      for {
        connection <- MongoNode.connection(c)
        collection <- MongoNode.collection(c)
        filter <- MongoNode.document(c, "filter", Some(Json.obj())).map(_.getOrElse(Json.obj()))
        allowAll <- c.getOrElse[Boolean]("allowAll")(false).left.map(_.message)
        _ <- Either.cond(allowAll || filter.asObject.exists(_.nonEmpty), (), "the filter is empty: set allowAll to update every document")
        update <- c.get[Json]("update").left.map(_ => "missing field 'update'")
        _ <- update.asObject match {
          case Some(o) => Either.cond(o.nonEmpty && o.keys.forall(_.startsWith("$")), (), "update is a document of operators, such as {\"$set\": {...}}, or a pipeline")
          case None    => Either.cond(update.asArray.exists(_.nonEmpty), (), "update is a document of operators, such as {\"$set\": {...}}, or a pipeline")
        }
        _ <- Template.problems(update).headOption.toLeft(())
        many <- c.getOrElse[Boolean]("many")(false).left.map(_.message)
        upsert <- c.getOrElse[Boolean]("upsert")(false).left.map(_.message)
        timeout <- MongoNode.timeout(c)
      } yield Params(connection, collection, filter, update, many, upsert, timeout)
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Update (MongoDB)",
    description = "Updates the first document that matches a filter, or all of them; with upsert it creates one when none matches.",
    config = ConfigSchema(
      List(
        MongoNode.connectionField,
        MongoNode.collectionField,
        MongoNode.documentField("filter", s"Which documents, such as {\"_id\": {\"$$path\": \"order.id\"}}. ${MongoNode.markersNote}", required = false),
        ConfigSchema.Field("update", DataType.AnyJson, description = Some("Operators, such as {\"$set\": {\"status\": \"paid\"}}, or a pipeline"), ui = Some(UiSpec.code("json", 6))),
        ConfigSchema.Field("many", DataType.Bool, required = false, default = Some(Json.False), description = Some("Update every document that matches, not only the first")),
        ConfigSchema.Field("upsert", DataType.Bool, required = false, default = Some(Json.False), description = Some("Create the document when none matches")),
        ConfigSchema.Field("allowAll", DataType.Bool, required = false, default = Some(Json.False), description = Some("Allow an empty filter")),
        MongoNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(MongoNode.affectedOutput),
    reads = json => params(json).toOption.toList.flatMap(p => MongoNode.reads(List(p.filter, p.update))),
    checkParams = json => params(json).left.toOption.toList
  )
}
