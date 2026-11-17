package xplat.flow.mongo

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Reads the documents of a collection that match `filter`, with an optional `projection`, `sort` and `skip`. It answers, in the
  * outputs it declares:
  *
  *  - `documents`: `list[{}]`, at most `limit` (the audit says if there were more);
  *  - `count`: how many came;
  *  - `first`: the first one, or `null`.
  */
final class FindNode[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit F: Async[F]) extends MongoNode[F](connections, pool) {

  val descriptor: NodeDescriptor = FindNode.descriptor

  def plan(input: NodeInput): Either[String, MongoNode.Plan] =
    for {
      p <- FindNode.params(input.params)
      filter <- MongoNode.resolve(p.filter, input)
      projection <- p.projection.traverse(MongoNode.resolve(_, input))
      sort <- p.sort.traverse(MongoNode.resolve(_, input))
    } yield MongoNode.Plan(p.connection, MongoNode.Op.Find(p.collection, filter, projection, sort, p.skip, p.limit), p.timeoutSeconds, s"mongo.find on '${p.connection}'.${p.collection}")
}

object FindNode {

  val Kind: String = "mongo.find"

  final case class Params(connection: String, collection: String, filter: Json, projection: Option[Json], sort: Option[Json], skip: Int, limit: Int, timeoutSeconds: Int)

  def params(raw: Json): Either[String, Params] =
    MongoNode.lenient(raw).flatMap { json =>
      val c = json.hcursor
      for {
        connection <- MongoNode.connection(c)
        collection <- MongoNode.collection(c)
        filter <- MongoNode.document(c, "filter", Some(Json.obj()))
        projection <- MongoNode.document(c, "projection", None).map(_.filter(_.asObject.exists(_.nonEmpty)))
        sort <- MongoNode.document(c, "sort", None).map(_.filter(_.asObject.exists(_.nonEmpty)))
        skip <- c.getOrElse[Int]("skip")(0).left.map(_.message).filterOrElse(_ >= 0, "skip cannot be negative")
        limit <- MongoNode.limit(c)
        timeout <- MongoNode.timeout(c)
      } yield Params(connection, collection, filter.getOrElse(Json.obj()), projection, sort, skip, limit, timeout)
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Find (MongoDB)",
    description = "Reads the documents of a collection that match a filter and writes them in the state as a list (documents, count, first). To stream them, follow it with a List source.",
    config = ConfigSchema(
      List(
        MongoNode.connectionField,
        MongoNode.collectionField,
        MongoNode.documentField("filter", s"Query filter, such as {\"status\": \"open\"}. ${MongoNode.markersNote}", required = false),
        MongoNode.documentField("projection", "Fields to keep or leave out, such as {\"name\": 1}", required = false, rows = 3),
        MongoNode.documentField("sort", "Order, such as {\"createdAt\": -1}", required = false, rows = 3),
        ConfigSchema.Field("skip", DataType.Num, required = false, default = Some(Json.fromInt(0)), description = Some("Documents to leave out at the start")),
        ConfigSchema.Field("limit", DataType.Num, required = false, default = Some(Json.fromInt(MongoNode.DefaultLimit)), description = Some("Most documents kept")),
        MongoNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Read,
    defaultOutputs = _ => List(MongoNode.documentsOutput),
    reads = json => params(json).toOption.toList.flatMap(p => MongoNode.reads(p.filter :: p.projection.toList ++ p.sort.toList)),
    checkParams = json => params(json).left.toOption.toList
  )
}
