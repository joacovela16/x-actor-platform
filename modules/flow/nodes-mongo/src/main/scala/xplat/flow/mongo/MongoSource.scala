package xplat.flow.mongo

import cats.effect.Async
import cats.syntax.all._
import com.mongodb.client.MongoCursor
import fs2.Stream
import io.circe.Json
import org.bson.BsonDocument
import xplat.flow.model._
import xplat.flow.nodes.Connections
import xplat.flow.stream.{Borrow, Elem, StreamContext, StreamPorts, StreamSource}


/** The documents of a collection that match `filter`, as a stream: they are read with a cursor, `batchSize` at a time, as the
  * stream asks for them, so a collection of any size goes through without being in memory and a slow sink holds back the reads.
  * Unlike `mongo.find` there is no `limit` (a `stream.take` after it does that, and closes the cursor). `filter`, `projection` and
  * `sort` take `${state.path}` like the other MongoDB nodes, read when the segment starts.
  */
final class MongoSource[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit F: Async[F]) extends StreamSource[F] {

  val descriptor: NodeDescriptor = MongoSource.descriptor

  def open(ctx: StreamContext[F]): Stream[F, Elem[F]] =
    MongoSource.params(ctx.params, ctx.input) match {
      case Left(problem) => Stream.raiseError[F](new IllegalArgumentException(problem))
      case Right(p) =>
        Stream.eval(connections.resolve(p.connection)).flatMap {
          case None => Stream.raiseError[F](new IllegalArgumentException(s"the connection '${p.connection}' does not exist"))
          case Some(c) =>
            Stream
              .resource(Borrow[F, com.mongodb.client.MongoDatabase](f => pool.use(c)(f)))
              .flatMap(db => Stream.bracket(F.blocking(MongoSource.cursor(db, p)))(cur => F.blocking(cur.close())))
              .flatMap(cur => Stream.fromBlockingIterator[F](MongoSource.iterator(cur), p.batchSize))
              .evalMap(d => ctx.now.map(Elem.of[F](MongoNode.fromBson(d), _)))
        }
    }
}

object MongoSource {
  val Kind: String = "stream.source.mongo"

  final case class Params(connection: String, collection: String, filter: Json, projection: Option[Json], sort: Option[Json], batchSize: Int)

  /** The configuration, with the `${state.path}` of its documents filled from what the segment received. */
  def params(raw: Json, input: NodeInput): Either[String, Params] =
    for {
      p <- shape(raw)
      filter <- MongoNode.resolve(p.filter, input)
      projection <- p.projection.traverse(MongoNode.resolve(_, input))
      sort <- p.sort.traverse(MongoNode.resolve(_, input))
    } yield p.copy(filter = filter, projection = projection, sort = sort)

  def shape(raw: Json): Either[String, Params] =
    MongoNode.lenient(raw).flatMap { json =>
      val c = json.hcursor
      for {
        connection <- MongoNode.connection(c)
        collection <- MongoNode.collection(c)
        filter <- MongoNode.document(c, "filter", Some(Json.obj()))
        projection <- MongoNode.document(c, "projection", None).map(_.filter(_.asObject.exists(_.nonEmpty)))
        sort <- MongoNode.document(c, "sort", None).map(_.filter(_.asObject.exists(_.nonEmpty)))
        batch <- c.getOrElse[Int]("batchSize")(500).left.map(_.message).filterOrElse(n => n > 0 && n <= 10000, "batchSize must be between 1 and 10000")
      } yield Params(connection, collection, filter.getOrElse(Json.obj()), projection, sort, batch)
    }

  private def cursor(db: com.mongodb.client.MongoDatabase, p: Params): MongoCursor[BsonDocument] = {
    var it = db.getCollection(p.collection, classOf[BsonDocument]).find(MongoNode.toBson(p.filter)).batchSize(p.batchSize)
    p.projection.foreach(j => it = it.projection(MongoNode.toBson(j)))
    p.sort.foreach(j => it = it.sort(MongoNode.toBson(j)))
    it.iterator()
  }

  private def iterator(cur: MongoCursor[BsonDocument]): Iterator[BsonDocument] = new Iterator[BsonDocument] {
    def hasNext: Boolean = cur.hasNext
    def next(): BsonDocument = cur.next()
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SourceCategory,
    label = "MongoDB",
    description = "Starts a stream with the documents of a collection that match a filter, read with a cursor as the stream asks for them: for collections of any size.",
    config = ConfigSchema(
      List(
        MongoNode.connectionField,
        MongoNode.collectionField,
        MongoNode.documentField("filter", s"Query filter, such as {\"status\": \"open\"}. ${MongoNode.markersNote}", required = false),
        MongoNode.documentField("projection", "Fields to keep or leave out, such as {\"name\": 1}", required = false, rows = 3),
        MongoNode.documentField("sort", "Order, such as {\"createdAt\": -1}", required = false, rows = 3),
        ConfigSchema.Field("batchSize", DataType.Num, required = false, default = Some(Json.fromInt(500)), description = Some("Documents read from the server at a time"))
      )
    ),
    sideEffects = SideEffects.Read,
    ports = _ => StreamPorts.output(DataType.Obj(Map.empty)),
    reads = json => shape(json).toOption.toList.flatMap(p => MongoNode.reads(p.filter :: p.projection.toList ++ p.sort.toList)),
    checkParams = json => shape(json).left.toOption.toList
  )
}
