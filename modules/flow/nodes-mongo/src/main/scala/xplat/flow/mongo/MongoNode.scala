package xplat.flow.mongo

import cats.effect.Async
import cats.syntax.all._
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.{DeleteOptions, UpdateOptions}
import io.circe.{ACursor, Json}
import org.bson.json.{JsonMode, JsonWriterSettings}
import org.bson.{BsonArray, BsonDocument, BsonValue}
import xplat.flow.nodes.Template
import xplat.flow.model._
import xplat.flow.nodes.{Connections, NodeType}

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/** What the MongoDB nodes have in common: they work out one operation from the configuration and the state (see [[Template]]), run
  * it on the client of the named connection, and answer the values that the node declared.
  *
  * Documents go in and come out as MongoDB Extended JSON in its relaxed form: numbers and text as such, an `ObjectId` as
  * `{"$oid": "..."}`, a date as `{"$date": "..."}`. What comes out can be put back into a filter as it is.
  *
  * A node that writes is simulated in test mode: it does not connect, and the audit shows the operation it would have run. A
  * write that must not be repeated by a retry can use `"${context.key}"` as its `_id`, or as a field with a unique index.
  *
  * Secrets of the connection never reach the answer: messages of the driver are cleaned of them.
  */
abstract class MongoNode[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit F: Async[F]) extends NodeType[F] {

  /** The operation to run for this input, or why the configuration cannot make one. */
  def plan(input: NodeInput): Either[String, MongoNode.Plan]

  def run(input: NodeInput): F[NodeOutput] =
    plan(input) match {
      case Left(problem) => NodeOutput.failed(problem).pure[F]
      case Right(p) =>
        connections.resolve(p.connection).flatMap {
          case None => NodeOutput.failed(s"the connection '${p.connection}' does not exist").pure[F]
          case Some(c) =>
            val started = System.nanoTime()
            pool
              .use(c)(db => F.blocking(MongoNode.work(db, p)))
              .map(outcome => MongoNode.answer(p, input, outcome, (System.nanoTime() - started) / 1000000))
              .handleError(e => NodeOutput.failed(MongoNode.clean(s"${p.describe} failed: ${Option(e.getMessage).getOrElse(e.toString)}", c.secrets)))
        }
    }

  override def simulated(input: NodeInput): Option[NodeOutput] =
    Some(plan(input) match {
      case Left(problem) => NodeOutput.failed(problem)
      case Right(p) =>
        val empty: MongoNode.Outcome = p.op match {
          case _: MongoNode.Op.Find | _: MongoNode.Op.Aggregate => MongoNode.Outcome.Read(Vector.empty, truncated = false)
          case _: MongoNode.Op.Insert                           => MongoNode.Outcome.Inserted(Vector.empty)
          case _: MongoNode.Op.Update                           => MongoNode.Outcome.Updated(0L, 0L, None)
          case _: MongoNode.Op.Delete                           => MongoNode.Outcome.Deleted(0L)
        }
        MongoNode.answer(p, input, empty, 0L).copy(info = MongoNode.info(p) + ("simulated" -> Json.True))
    })
}

object MongoNode {

  /** One operation on one collection. The documents are already resolved: they have no markers left. */
  sealed trait Op { def collection: String }
  object Op {
    final case class Find(collection: String, filter: Json, projection: Option[Json], sort: Option[Json], skip: Int, limit: Int) extends Op
    final case class Aggregate(collection: String, pipeline: Vector[Json], limit: Int) extends Op
    final case class Insert(collection: String, documents: Vector[Json]) extends Op
    final case class Update(collection: String, filter: Json, update: Json, many: Boolean, upsert: Boolean) extends Op
    final case class Delete(collection: String, filter: Json, many: Boolean) extends Op
  }

  /** @param describe what is run, for messages: `mongo.find on 'main'.orders`. */
  final case class Plan(connection: String, op: Op, timeoutSeconds: Int, describe: String)

  sealed trait Outcome
  object Outcome {
    final case class Read(documents: Vector[Json], truncated: Boolean) extends Outcome
    final case class Inserted(ids: Vector[Json]) extends Outcome
    final case class Updated(matched: Long, modified: Long, upsertedId: Option[Json]) extends Outcome
    final case class Deleted(deleted: Long) extends Outcome
  }

  val DefaultLimit: Int = 1000
  val MaxLimit: Int = 100000
  val DefaultTimeoutSeconds: Int = 30

  private val relaxed: JsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()

  def toBson(json: Json): BsonDocument = BsonDocument.parse(json.noSpaces)

  private def toBsonArray(values: Vector[Json]): BsonArray = BsonDocument.parse(Json.obj("a" -> Json.fromValues(values)).noSpaces).getArray("a")

  def fromBson(value: BsonValue): Json = {
    val wrapped = new BsonDocument("v", value).toJson(relaxed)
    io.circe.parser.parse(wrapped).toOption.flatMap(_.hcursor.downField("v").focus).getOrElse(Json.Null)
  }

  private def work(db: MongoDatabase, p: Plan): Outcome = {
    val coll = db.getCollection(p.op.collection, classOf[BsonDocument])
    p.op match {
      case o: Op.Find =>
        var it = coll.find(toBson(o.filter)).skip(o.skip).limit(o.limit + 1).maxTime(p.timeoutSeconds.toLong, TimeUnit.SECONDS)
        o.projection.foreach(j => it = it.projection(toBson(j)))
        o.sort.foreach(j => it = it.sort(toBson(j)))
        val docs = it.into(new java.util.ArrayList[BsonDocument]()).asScala.toVector
        Outcome.Read(docs.take(o.limit).map(fromBson), docs.size > o.limit)
      case o: Op.Aggregate =>
        val stages = o.pipeline.map(toBson).toList.asJava
        val docs = coll.aggregate(stages).maxTime(p.timeoutSeconds.toLong, TimeUnit.SECONDS).batchSize(math.min(o.limit + 1, 1000)).iterator()
        try {
          val out = Vector.newBuilder[Json]
          var n = 0
          while (n <= o.limit && docs.hasNext) { val d = docs.next(); if (n < o.limit) out += fromBson(d); n += 1 }
          Outcome.Read(out.result(), n > o.limit)
        } finally docs.close()
      case o: Op.Insert =>
        val docs = toBsonArray(o.documents).getValues.asScala.map(_.asDocument()).toList
        val result = coll.insertMany(docs.asJava)
        Outcome.Inserted(result.getInsertedIds.asScala.toList.sortBy(_._1.intValue).map(kv => fromBson(kv._2)).toVector)
      case o: Op.Update =>
        val options = new UpdateOptions().upsert(o.upsert)
        val filter = toBson(o.filter)
        val result = o.update.asArray match {
          case Some(stages) =>
            val pipeline = stages.map(toBson).toList.asJava
            if (o.many) coll.updateMany(filter, pipeline, options) else coll.updateOne(filter, pipeline, options)
          case None => if (o.many) coll.updateMany(filter, toBson(o.update), options) else coll.updateOne(filter, toBson(o.update), options)
        }
        Outcome.Updated(result.getMatchedCount, result.getModifiedCount, Option(result.getUpsertedId).map(fromBson))
      case o: Op.Delete =>
        val filter = toBson(o.filter)
        val result = if (o.many) coll.deleteMany(filter, new DeleteOptions()) else coll.deleteOne(filter, new DeleteOptions())
        Outcome.Deleted(result.getDeletedCount)
    }
  }

  /** Only the values the node declared, which is what the engine accepts. A node that declares nothing answers all of them. */
  private def answer(p: Plan, input: NodeInput, outcome: Outcome, millis: Long): NodeOutput = {
    val all: Map[String, Json] = outcome match {
      case Outcome.Read(docs, _) => Map("documents" -> Json.fromValues(docs), "count" -> Json.fromInt(docs.size), "first" -> docs.headOption.getOrElse(Json.Null))
      case Outcome.Inserted(ids) => Map("insertedIds" -> Json.fromValues(ids), "insertedId" -> ids.headOption.getOrElse(Json.Null), "affected" -> Json.fromInt(ids.size))
      case Outcome.Updated(matched, modified, upserted) =>
        Map("matched" -> Json.fromLong(matched), "affected" -> Json.fromLong(modified), "upsertedId" -> upserted.getOrElse(Json.Null))
      case Outcome.Deleted(n) => Map("affected" -> Json.fromLong(n))
    }
    val wanted = input.outputs.map(_.name)
    val unknown = wanted.filterNot(all.contains)
    if (unknown.nonEmpty) NodeOutput.failed(s"${p.describe} cannot answer ${unknown.map(n => s"'$n'").mkString(", ")}; it answers ${all.keys.toList.sorted.mkString(", ")}")
    else {
      val values = if (wanted.isEmpty) all.filter { case (k, _) => k != "first" && k != "insertedId" } else all.filter { case (k, _) => wanted.contains(k) }
      val extra: Map[String, Json] = outcome match {
        case Outcome.Read(docs, truncated) => Map("documents" -> Json.fromInt(docs.size), "truncated" -> Json.fromBoolean(truncated))
        case Outcome.Inserted(ids)         => Map("affected" -> Json.fromInt(ids.size))
        case Outcome.Updated(m, n, _)      => Map("matched" -> Json.fromLong(m), "affected" -> Json.fromLong(n))
        case Outcome.Deleted(n)            => Map("affected" -> Json.fromLong(n))
      }
      NodeOutput(values, info = info(p) ++ extra + ("durationMs" -> Json.fromLong(millis)))
    }
  }

  private def info(p: Plan): Map[String, Json] = {
    val operation: Json = p.op match {
      case o: Op.Find      => Json.obj("find" -> o.filter, "projection" -> o.projection.getOrElse(Json.Null), "sort" -> o.sort.getOrElse(Json.Null), "skip" -> Json.fromInt(o.skip), "limit" -> Json.fromInt(o.limit))
      case o: Op.Aggregate => Json.obj("aggregate" -> Json.fromValues(o.pipeline))
      case o: Op.Insert    => Json.obj("insert" -> Json.fromValues(o.documents))
      case o: Op.Update    => Json.obj("update" -> o.update, "filter" -> o.filter, "many" -> Json.fromBoolean(o.many), "upsert" -> Json.fromBoolean(o.upsert))
      case o: Op.Delete    => Json.obj("delete" -> o.filter, "many" -> Json.fromBoolean(o.many))
    }
    Map("connection" -> Json.fromString(p.connection), "collection" -> Json.fromString(p.op.collection), "operation" -> operation)
  }

  private def clean(text: String, secrets: Map[String, String]): String =
    secrets.values.filter(_.nonEmpty).foldLeft(text)((t, secret) => t.replace(secret, "***"))

  // ---- Reading the configuration ----

  /** The fields that hold documents or pipelines. The editor sends them as text; written by hand they can also be JSON. */
  val DocumentFields: List[String] = List("filter", "projection", "sort", "pipeline", "document", "documents", "update")

  /** `params` with every field of `fields` that is text read as a document, in the syntax of the MongoDB shell: keys without quotes
    * (`{status: "open", $path: "order.id"}`), single quotes, `ObjectId("...")`, `ISODate("...")`. Empty text is no value.
    */
  def lenient(params: Json, fields: List[String] = DocumentFields): Either[String, Json] =
    params.asObject match {
      case None => Right(params)
      case Some(o) =>
        fields
          .foldLeft[Either[String, io.circe.JsonObject]](Right(o)) { (acc, name) =>
            acc.flatMap { obj =>
              obj(name).flatMap(_.asString) match {
                case None                       => Right(obj)
                case Some(text) if text.trim.isEmpty => Right(obj.remove(name))
                case Some(text)                 => parseText(text).left.map(e => s"$name is not valid: $e").map(j => obj.add(name, j))
              }
            }
          }
          .map(Json.fromJsonObject)
    }

  /** Text in the syntax of the MongoDB shell, as JSON (relaxed Extended JSON). */
  def parseText(text: String): Either[String, Json] =
    scala.util.Try(BsonDocument.parse(s"{\"v\": $text\n}")).toEither
      .left
      .map(e => Option(e.getMessage).getOrElse(e.toString).replace("JSON reader", "parser"))
      .flatMap(doc => if (doc.size != 1) Left("there is something after the value") else Right(fromBson(doc.get("v"))))

  def connection(c: ACursor): Either[String, String] = c.get[String]("connection").left.map(_ => "missing field 'connection'").filterOrElse(_.nonEmpty, "connection is empty")

  /** A collection name: not empty, no `$`, no NUL, not one of the `system.` collections. */
  def collection(c: ACursor): Either[String, String] =
    c.get[String]("collection")
      .left
      .map(_ => "missing field 'collection'")
      .filterOrElse(_.nonEmpty, "collection is empty")
      .filterOrElse(n => !n.contains('$') && !n.contains('\u0000'), "a collection name cannot have '$'")
      .filterOrElse(n => !n.startsWith("system."), "the system collections cannot be used")

  def timeout(c: ACursor): Either[String, Int] =
    c.getOrElse[Int]("timeoutSeconds")(DefaultTimeoutSeconds).left.map(_.message).filterOrElse(t => t > 0 && t <= 3600, "timeoutSeconds must be between 1 and 3600")

  def limit(c: ACursor): Either[String, Int] =
    c.getOrElse[Int]("limit")(DefaultLimit).left.map(_.message).filterOrElse(n => n > 0 && n <= MaxLimit, s"limit must be between 1 and $MaxLimit")

  /** An object field (a document); missing means `default`. */
  def document(c: ACursor, field: String, default: Option[Json]): Either[String, Option[Json]] =
    c.downField(field).focus.filterNot(_.isNull).orElse(default) match {
      case None                    => Right(None)
      case Some(j) if j.isObject   => Template.problems(j).headOption.toLeft(Some(j))
      case Some(_)                 => Left(s"$field must be an object")
    }

  /** A template resolved and checked to be something the driver can read as BSON. */
  def resolve(template: Json, input: NodeInput): Either[String, Json] =
    Template.resolve(template, input).flatMap(j => scala.util.Try(if (j.isObject) toBson(j) else toBsonArray(Vector(j))).toEither.left.map(e => s"not a valid document: ${e.getMessage}").as(j))

  val connectionField: ConfigSchema.Field = ConfigSchema.Field("connection", DataType.Str, description = Some("Name of the MongoDB connection"))
  val collectionField: ConfigSchema.Field = ConfigSchema.Field("collection", DataType.Str, description = Some("Name of the collection"))
  val timeoutField: ConfigSchema.Field =
    ConfigSchema.Field("timeoutSeconds", DataType.Num, required = false, default = Some(Json.fromInt(DefaultTimeoutSeconds)), description = Some("Time limit of the operation"))

  /** A field that holds a document with markers. */
  def documentField(name: String, description: String, required: Boolean = true, rows: Int = 6): ConfigSchema.Field =
    ConfigSchema.Field(name, DataType.AnyJson, required = required, default = if (required) None else Some(Json.obj()), description = Some(description), ui = Some(UiSpec.code("json", rows)))

  val markersNote: String = "Shell syntax: keys without quotes. Values of the run: \"${state.order.id}\" (keeps its type), \"${context.key}\"."

  def reads(templates: List[Json]): List[StateRead] = templates.flatMap(Template.reads).distinct.map(StateRead(_, DataType.AnyJson))

  val documentsOutput: OutputDecl = OutputDecl("documents", StatePath(List("documents")), DataType.ListOf(DataType.Obj(Map.empty)))
  val affectedOutput: OutputDecl = OutputDecl("affected", StatePath(List("affected")), DataType.Num)
}
