package xplat.extensions.persistence.mongodb

import cats.effect.kernel.{Async, Resource}
import cats.implicits._
import com.mongodb.client.model._
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import com.mongodb.{ErrorCategory, MongoWriteException}
import org.bson.types.Binary
import org.bson.{Document => BsonDocument}
import xplat.core.io.coordination.LeaseStore
import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}
import xplat.core.persistence.api.{Journal, JournalBuilder}

import java.util.{Arrays => JArrays, Date}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** @param uri
  *   connection string, e.g. `mongodb://user:pass@host:27017`.
  * @param collectionPrefix
  *   prefix of the collections (`<prefix>events`, `<prefix>snapshots`, `<prefix>leases`).
  */
final case class MongoConfig(uri: String, database: String, collectionPrefix: String = "xplat_")

/** Journal and lease store on MongoDB (4.2 or later).
  *
  * Each write is one document holding the whole batch of events, so it is atomic without transactions. A unique index
  * on the first offset of a batch makes a concurrent writer of the same entity fail instead of interleaving. Lease
  * expiry uses the server clock (`$$NOW`), so nodes with skewed clocks still agree.
  */
final class MongoPersistence[F[_]] private (client: MongoClient, config: MongoConfig)(implicit A: Async[F]) {

  private val db = client.getDatabase(config.database)
  private val events: MongoCollection[BsonDocument] = db.getCollection(s"${config.collectionPrefix}events")
  private val snapshots: MongoCollection[BsonDocument] = db.getCollection(s"${config.collectionPrefix}snapshots")
  private val leases: MongoCollection[BsonDocument] = db.getCollection(s"${config.collectionPrefix}leases")

  private def doc(pairs: (String, Any)*): BsonDocument = {
    val d = new BsonDocument()
    pairs.foreach { case (k, v) => d.append(k, v) }
    d
  }

  private def list(values: Any*): java.util.List[Any] = JArrays.asList(values: _*)

  private[mongodb] def createIndexes: F[Unit] = A.blocking {
    events.createIndex(Indexes.ascending("shard", "entity_id", "seq_from"), new IndexOptions().unique(true))
    events.createIndex(Indexes.ascending("shard", "entity_id", "seq_to"))
    ()
  }

  // ---- Journal ----

  private def entityFilter(shard: Shard): BsonDocument = doc("shard" -> shard.name, "entity_id" -> shard.id)
  private def snapshotId(shard: Shard): BsonDocument = doc("shard" -> shard.name, "entity_id" -> shard.id)

  private def lastSeq(shard: Shard): Long =
    Option(events.find(entityFilter(shard)).sort(Sorts.descending("seq_to")).limit(1).first()).map(_.getLong("seq_to").longValue()).getOrElse(0L)

  private def snapshotSeq(shard: Shard): Long =
    Option(snapshots.find(doc("_id" -> snapshotId(shard))).first()).map(_.getLong("seq").longValue()).getOrElse(0L)

  val journal: Journal[F] = new Journal[F] {

    def create(shard: Shard): F[Unit] = A.unit

    def getEvents(shard: Shard): F[fs2.Stream[F, JournalEvent]] =
      A.blocking(snapshotSeq(shard)).map { from =>
        val query = A.blocking(events.find(entityFilter(shard).append("seq_to", doc("$gt" -> from))).sort(Sorts.ascending("seq_from")).iterator())
        fs2.Stream
          .bracket(query)(cursor => A.blocking(cursor.close()))
          .flatMap(cursor => fs2.Stream.fromBlockingIterator[F](cursor.asScala, 16))
          .flatMap { batch =>
            val first = batch.getLong("seq_from").longValue()
            val at = batch.getLong("created_at").longValue()
            val payloads = batch.getList("events", classOf[Binary]).asScala.toList
            fs2.Stream.emits(payloads.zipWithIndex.collect {
              case (data, i) if first + i > from => JournalEvent(shard.name, shard.id, first + i, at, data.getData)
            })
          }
      }

    def getSnapshot(shard: Shard): F[Option[JournalSnapshot]] = A.blocking {
      Option(snapshots.find(doc("_id" -> snapshotId(shard))).first()).map { d =>
        val seq = d.getLong("seq").longValue()
        JournalSnapshot(shard.name, shard.id, seq, seq, Some(d.get("data", classOf[Binary]).getData))
      }
    }

    def writeSnapshot(shard: Shard, data: Array[Byte]): F[Unit] = A.realTime.flatMap { at =>
      A.blocking {
        val seq = lastSeq(shard)
        snapshots.replaceOne(
          doc("_id" -> snapshotId(shard)),
          doc("_id" -> snapshotId(shard), "seq" -> seq, "created_at" -> at.toMillis, "data" -> new Binary(data)),
          new ReplaceOptions().upsert(true)
        )
        ()
      }
    }

    def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): F[Unit] =
      if (entries.isEmpty) A.unit
      else
        A.realTime.flatMap { at =>
          A.blocking {
            val last = lastSeq(shard)
            events.insertOne(
              entityFilter(shard)
                .append("seq_from", last + 1)
                .append("seq_to", last + entries.size)
                .append("created_at", at.toMillis)
                .append("events", entries.map(new Binary(_)).asJava)
            )
            ()
          }
        }

    /** The client is shared by every entity and closed with the resource that created it. */
    def close(): F[Unit] = A.unit
  }

  val journalBuilder: JournalBuilder[F] = () => A.pure(journal)

  // ---- Leases ----

  private val now = "$$NOW"
  private def expiresIn(ttl: FiniteDuration): BsonDocument = doc("$add" -> list(now, ttl.toMillis))
  private def heldBy(name: String, owner: String): BsonDocument =
    doc("_id" -> name, "$expr" -> doc("$and" -> list(doc("$eq" -> list("$owner", owner)), doc("$gt" -> list("$expires_at", now)))))

  val leaseStore: LeaseStore[F] = new LeaseStore[F] {

    def tryAcquire(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] = A.blocking {
      val free = doc("_id" -> name, "$expr" -> doc("$or" -> list(doc("$eq" -> list("$owner", owner)), doc("$lte" -> list("$expires_at", now)))))
      val take = JArrays.asList(doc("$set" -> doc("owner" -> owner, "expires_at" -> expiresIn(timeToLive))))
      // Taken over if it is ours or expired; `$expr` is not allowed in an upsert, so creating it is a second step.
      leases.updateOne(free, take).getMatchedCount == 1 || {
        try {
          // Every lease document has an owner, so this only matches when there is none and inserts it.
          leases.updateOne(doc("_id" -> name, "owner" -> doc("$exists" -> false)), take, new UpdateOptions().upsert(true)).getUpsertedId != null
        } catch {
          // Someone else holds it: the insert collided with the existing document.
          case e: MongoWriteException if e.getError.getCategory == ErrorCategory.DUPLICATE_KEY => false
        }
      }
    }

    def renew(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] = A.blocking {
      leases.updateOne(heldBy(name, owner), JArrays.asList(doc("$set" -> doc("expires_at" -> expiresIn(timeToLive))))).getMatchedCount == 1
    }

    def release(name: String, owner: String): F[Boolean] = A.blocking(leases.deleteOne(heldBy(name, owner)).getDeletedCount == 1)

    def holder(name: String): F[Option[String]] = A.blocking {
      Option(leases.find(doc("_id" -> name, "$expr" -> doc("$gt" -> list("$expires_at", now)))).first()).map(_.getString("owner"))
    }
  }

  private[mongodb] def serverTime: F[Date] = A.blocking(db.runCommand(doc("hello" -> 1)).getDate("localTime"))
}

object MongoPersistence {

  def resource[F[_]](config: MongoConfig)(implicit A: Async[F]): Resource[F, MongoPersistence[F]] =
    Resource
      .make(A.blocking(MongoClients.create(config.uri)))(c => A.blocking(c.close()))
      .evalMap(client => A.pure(new MongoPersistence[F](client, config)).flatTap(_.createIndexes))
}
