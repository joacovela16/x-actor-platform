package xplat.flow.server

import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.mongodb.client.model.{Filters, ReplaceOptions}
import com.mongodb.client.{MongoClients, MongoCollection}
import io.circe.{Decoder, Encoder}
import org.bson.Document
import xplat.flow.http.JsonSlot

import java.util.Date

/** Where the stores of the server keep what they hold when the database is MongoDB: one document for each store in one collection,
  * `{_id: <name>, json: <text>, updatedAt: <date>}`. The value is kept as JSON text, not as fields, because flows have keys that
  * MongoDB does not allow as field names (`$set`, `a.b`...).
  */
final class MongoState[F[_]] private (collection: MongoCollection[Document])(implicit A: Async[F]) {

  def slot(name: String): JsonSlot[F] = new JsonSlot[F] {
    def read[V: Decoder](empty: => V): F[V] =
      A.blocking(Option(collection.find(Filters.eq("_id", name)).first()).map(_.getString("json"))).flatMap {
        case None       => A.delay(empty)
        case Some(text) => A.fromEither(io.circe.parser.decode[V](text).leftMap(e => new RuntimeException(s"cannot read '$name' from MongoDB: ${e.getMessage}", e)))
      }

    def write[V: Encoder](value: V): F[Unit] =
      A.blocking {
        val doc = new Document("_id", name).append("json", Encoder[V].apply(value).noSpaces).append("updatedAt", new Date())
        collection.replaceOne(Filters.eq("_id", name), doc, new ReplaceOptions().upsert(true))
        ()
      }
  }
}

object MongoState {

  def resource[F[_]](uri: String, database: String, collection: String)(implicit A: Async[F]): Resource[F, MongoState[F]] =
    Resource
      .make(A.blocking(MongoClients.create(uri)))(c => A.blocking(c.close()))
      .map(client => new MongoState[F](client.getDatabase(database).getCollection(collection)))
}
