package xplat.flow.mongo

import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.mongodb.client.model.changestream.{ChangeStreamDocument, FullDocument}
import com.mongodb.client.model.{Filters, ReplaceOptions}
import com.mongodb.client.{MongoChangeStreamCursor, MongoClients, MongoDatabase}
import io.circe.Json
import org.bson.{BsonDocument, Document}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import xplat.core.connectors.{Consuming, Incoming, RetryPolicy, RunningConsumer, Sink}
import xplat.flow.nodes.Connections

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

/** Reads the changes of a collection (a MongoDB change stream) as messages, for a `queue` trigger whose connection is `mongodb` and
  * whose queue is the collection. MongoDB must run as a replica set (one member is enough), which is what change streams need.
  *
  * Each change is a message:
  *
  *  - `body`: `{operationType, documentKey, fullDocument, updateDescription, clusterTime}` in relaxed Extended JSON. `fullDocument`
  *    is the document after the change (looked up for updates; `null` for deletes);
  *  - `key`: the `_id` of the document, as JSON text;
  *  - `headers`: `operation-type`;
  *  - `position`: the resume token of the change, so a change that is delivered again starts no second instance.
  *
  * Where it has read up to is kept in the collection `_flow_resume` of the same database, one document per consumer group. After a
  * restart it goes on from there, as long as the oplog still has it; the first time it starts from now.
  */
object ChangeStreams {

  val ResumeCollection: String = "_flow_resume"

  private val operations = List("insert", "update", "replace", "delete")

  def consume[F[_]](connection: Connections.Resolved, collection: String, group: String, sink: Sink[F, Array[Byte]], policy: RetryPolicy = RetryPolicy())(implicit
      A: Async[F]
  ): Resource[F, RunningConsumer[F]] =
    for {
      log <- Resource.eval(Slf4jLogger.fromName[F]("xplat.flow.mongo"))
      settings <- Resource.eval(A.fromEither(MongoKinds.client(connection).leftMap(new IllegalArgumentException(_))))
      _ <- Resource.eval(A.raiseError[Unit](new IllegalArgumentException("a change stream needs a collection")).whenA(collection.trim.isEmpty))
      client <- Resource.make(A.blocking(MongoClients.create(settings.settings)))(c => A.blocking(c.close()))
      db = client.getDatabase(settings.database)
      cursor <- Resource.make(A.blocking(open(db, collection, group)))(c => A.blocking(c.close()))
      running <- Consuming.background[F] {
        val deliver = Consuming.deliver[F, Array[Byte]](sink, policy, log) _
        def loop: F[Unit] =
          A.blocking(Option(cursor.tryNext())).flatMap {
            case None => A.cede
            case Some(change) =>
              val incoming = message(collection, change)
              deliver(collection, incoming.position, Right(incoming)) >> A.blocking(saveToken(db, group, change.getResumeToken))
          } >> loop
        loop
      }
    } yield running

  private def open(db: MongoDatabase, collection: String, group: String): MongoChangeStreamCursor[ChangeStreamDocument[Document]] = {
    val pipeline = java.util.List.of(new Document("$match", new Document("operationType", new Document("$in", java.util.List.of(operations: _*)))))
    val stream = db.getCollection(collection).watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).maxAwaitTime(1, TimeUnit.SECONDS)
    val token = Option(db.getCollection(ResumeCollection).find(Filters.eq("_id", group)).first()).flatMap(d => Option(d.get("token", classOf[Document])))
    token.fold(stream)(t => stream.resumeAfter(BsonDocument.parse(t.toJson))).cursor()
  }

  private def saveToken(db: MongoDatabase, group: String, token: BsonDocument): Unit = {
    db.getCollection(ResumeCollection).replaceOne(Filters.eq("_id", group), new Document("_id", group).append("token", Document.parse(token.toJson)), new ReplaceOptions().upsert(true))
    ()
  }

  private def message(collection: String, change: ChangeStreamDocument[Document]): Incoming[Array[Byte]] = {
    def doc(d: BsonDocument): Json = Option(d).fold(Json.Null)(MongoNode.fromBson)
    val id = Option(change.getDocumentKey).flatMap(k => Option(k.get("_id"))).map(MongoNode.fromBson)
    val body = Json.obj(
      "operationType" -> Json.fromString(change.getOperationTypeString),
      "documentKey" -> doc(change.getDocumentKey),
      "fullDocument" -> Option(change.getFullDocument).fold(Json.Null)(d => MongoNode.fromBson(d.toBsonDocument())),
      "updateDescription" -> Option(change.getUpdateDescription).fold(Json.Null) { u =>
        Json.obj(
          "updatedFields" -> doc(u.getUpdatedFields),
          "removedFields" -> Option(u.getRemovedFields).fold(Json.arr())(fs => Json.fromValues(fs.toArray.toList.map(f => Json.fromString(f.toString))))
        )
      },
      "clusterTime" -> Option(change.getClusterTime).fold(Json.Null)(MongoNode.fromBson)
    )
    Incoming(
      source = collection,
      key = id.map(_.noSpaces),
      value = body.noSpaces.getBytes(UTF_8),
      headers = Map("operation-type" -> change.getOperationTypeString),
      position = change.getResumeToken.getString("_data").getValue
    )
  }
}
