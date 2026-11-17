package xplat.flow.mongo

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import xplat.flow.nodes.Template
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Inserts `document`, or each document of `documents` (a list, or a `"${state.path}"` to a list of the state). It answers `affected`
  * and `insertedIds` (with `insertedId`, the first). A document without `_id` gets an `ObjectId`.
  */
final class InsertNode[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit F: Async[F]) extends MongoNode[F](connections, pool) {

  val descriptor: NodeDescriptor = InsertNode.descriptor

  def plan(input: NodeInput): Either[String, MongoNode.Plan] =
    for {
      p <- InsertNode.params(input.params)
      resolved <- Template.resolve(p.documents, input)
      docs <- resolved.asArray.orElse(resolved.asObject.map(_ => Vector(resolved))).toRight("documents must be a document or a list of documents")
      _ <- Either.cond(docs.nonEmpty, (), "there are no documents to insert")
      _ <- Either.cond(docs.forall(_.isObject), (), "every document to insert must be an object")
      checked <- docs.traverse(MongoNode.resolve(_, input))
    } yield MongoNode.Plan(p.connection, MongoNode.Op.Insert(p.collection, checked), p.timeoutSeconds, s"mongo.insert into '${p.connection}'.${p.collection}")
}

object InsertNode {

  val Kind: String = "mongo.insert"

  final case class Params(connection: String, collection: String, documents: Json, timeoutSeconds: Int)

  def params(raw: Json): Either[String, Params] =
    MongoNode.lenient(raw).flatMap { json =>
      val c = json.hcursor
      for {
        connection <- MongoNode.connection(c)
        collection <- MongoNode.collection(c)
        documents <- (c.downField("document").focus.filterNot(_.isNull), c.downField("documents").focus.filterNot(_.isNull)) match {
          case (Some(d), None) if d.isObject                  => Right(d)
          case (None, Some(ds)) if ds.isArray || ds.isObject  => Right(ds)
          case (Some(_), Some(_))                             => Left("give document or documents, not both")
          case (None, None)                                   => Left("missing field 'document' (or 'documents')")
          case _                                              => Left("document must be an object and documents a list")
        }
        _ <- Template.problems(documents).headOption.toLeft(())
        timeout <- MongoNode.timeout(c)
      } yield Params(connection, collection, documents, timeout)
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Insert (MongoDB)",
    description = "Inserts one document, or each document of a list, into a collection.",
    config = ConfigSchema(
      List(
        MongoNode.connectionField,
        MongoNode.collectionField,
        MongoNode.documentField("document", s"The document to insert. ${MongoNode.markersNote}", required = false, rows = 8),
        ConfigSchema.Field("documents", DataType.AnyJson, required = false, description = Some("Several documents instead: a list, or {\"$path\": \"state.list\"}"), ui = Some(UiSpec.code("json", 4))),
        MongoNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(MongoNode.affectedOutput),
    reads = json => params(json).toOption.toList.flatMap(p => MongoNode.reads(List(p.documents))),
    checkParams = json => params(json).left.toOption.toList
  )
}
