package xplat.flow.mongo

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import xplat.flow.nodes.Template
import xplat.flow.model._
import xplat.flow.nodes.Connections

/** Runs an aggregation `pipeline` on a collection and answers its documents, like [[FindNode]] (`documents`, `count`, `first`).
  * It only reads: a pipeline with `$out` or `$merge` is refused when the flow is saved.
  */
final class AggregateNode[F[_]](connections: Connections[F], pool: MongoPool[F])(implicit F: Async[F]) extends MongoNode[F](connections, pool) {

  val descriptor: NodeDescriptor = AggregateNode.descriptor

  def plan(input: NodeInput): Either[String, MongoNode.Plan] =
    for {
      p <- AggregateNode.params(input.params)
      stages <- p.pipeline.traverse(MongoNode.resolve(_, input))
    } yield MongoNode.Plan(p.connection, MongoNode.Op.Aggregate(p.collection, stages, p.limit), p.timeoutSeconds, s"mongo.aggregate on '${p.connection}'.${p.collection}")
}

object AggregateNode {

  val Kind: String = "mongo.aggregate"

  private val Writes: Set[String] = Set("$out", "$merge")

  final case class Params(connection: String, collection: String, pipeline: Vector[Json], limit: Int, timeoutSeconds: Int)

  def params(raw: Json): Either[String, Params] =
    MongoNode.lenient(raw).flatMap { json =>
      val c = json.hcursor
      for {
        connection <- MongoNode.connection(c)
        collection <- MongoNode.collection(c)
        pipeline <- c.get[Vector[Json]]("pipeline").left.map(_ => "pipeline must be a list of stages, such as [{$match: {status: \"open\"}}]")
        _ <- pipeline.traverse_(s => Either.cond(s.asObject.exists(_.size == 1), (), "each stage of the pipeline is an object with one key, such as {\"$match\": {...}}"))
        _ <- Either.cond(!pipeline.exists(_.asObject.exists(_.keys.exists(Writes))), (), "mongo.aggregate only reads: $out and $merge are not allowed")
        _ <- pipeline.flatMap(Template.problems).headOption.toLeft(())
        limit <- MongoNode.limit(c)
        timeout <- MongoNode.timeout(c)
      } yield Params(connection, collection, pipeline, limit, timeout)
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Database",
    label = "Aggregate (MongoDB)",
    description = "Runs an aggregation pipeline on a collection and writes the documents in the state (documents, count, first).",
    config = ConfigSchema(
      List(
        MongoNode.connectionField,
        MongoNode.collectionField,
        ConfigSchema.Field(
          "pipeline",
          DataType.AnyJson,
          default = Some(Json.arr()),
          description = Some(s"Stages, such as [{\"$$match\": {...}}, {\"$$group\": {...}}]. ${MongoNode.markersNote}"),
          ui = Some(UiSpec.code("json", 10))
        ),
        ConfigSchema.Field("limit", DataType.Num, required = false, default = Some(Json.fromInt(MongoNode.DefaultLimit)), description = Some("Most documents kept")),
        MongoNode.timeoutField
      )
    ),
    sideEffects = SideEffects.Read,
    defaultOutputs = _ => List(MongoNode.documentsOutput),
    reads = json => params(json).toOption.toList.flatMap(p => MongoNode.reads(p.pipeline.toList)),
    checkParams = json => params(json).left.toOption.toList
  )
}
