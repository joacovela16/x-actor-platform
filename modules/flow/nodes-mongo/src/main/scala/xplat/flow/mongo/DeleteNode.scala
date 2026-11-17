package xplat.flow.mongo

import cats.effect.Async
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Deletes the first document that matches `filter` (or all of them, with `many`). An empty filter is refused unless `allowAll` is
  * set. It answers `affected`.
  */
final class DeleteNode[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit F: Async[F]) extends MongoNode[F](connections, pool) {

  val descriptor: NodeDescriptor = DeleteNode.descriptor

  def plan(input: NodeInput): Either[String, MongoNode.Plan] =
    for {
      p <- DeleteNode.params(input.params)
      filter <- MongoNode.resolve(p.filter, input)
    } yield MongoNode.Plan(p.connection, MongoNode.Op.Delete(p.collection, filter, p.many), p.timeoutSeconds, s"mongo.delete on '${p.connection}'.${p.collection}")
}

object DeleteNode {

  val Kind: String = "mongo.delete"

  final case class Params(connection: String, collection: String, filter: Json, many: Boolean, timeoutSeconds: Int)

  def params(raw: Json): Either[String, Params] =
    MongoNode.lenient(raw).flatMap { json =>
      val c = json.hcursor
      for {
        connection <- MongoNode.connection(c)
        collection <- MongoNode.collection(c)
        filter <- MongoNode.document(c, "filter", Some(Json.obj())).map(_.getOrElse(Json.obj()))
        allowAll <- c.getOrElse[Boolean]("allowAll")(false).left.map(_.message)
        _ <- Either.cond(allowAll || filter.asObject.exists(_.nonEmpty), (), "the filter is empty: set allowAll to delete every document")
        many <- c.getOrElse[Boolean]("many")(false).left.map(_.message)
        timeout <- MongoNode.timeout(c)
      } yield Params(connection, collection, filter, many, timeout)
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Delete (MongoDB)",
    description = "Deletes the first document that matches a filter, or all of them.",
    config = ConfigSchema(
      List(
        MongoNode.connectionField,
        MongoNode.collectionField,
        MongoNode.documentField("filter", s"Which documents. ${MongoNode.markersNote}", required = false),
        ConfigSchema.Field("many", DataType.Bool, required = false, default = Some(Json.False), description = Some("Delete every document that matches, not only the first")),
        ConfigSchema.Field("allowAll", DataType.Bool, required = false, default = Some(Json.False), description = Some("Allow an empty filter")),
        MongoNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(MongoNode.affectedOutput),
    reads = json => params(json).toOption.toList.flatMap(p => MongoNode.reads(List(p.filter))),
    checkParams = json => params(json).left.toOption.toList
  )
}
