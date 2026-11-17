package xplat.flow.http

import io.circe.Json
import xplat.flow.model.{Refs, StatePath}

/** An HTTP request that a trigger received, as JSON, and the inputs of the flow that come out of it. */
object HookRequest {

  /** `body` is the JSON of the body if it is JSON, the text if it is not, and null if it is empty. */
  def json(method: String, path: String, query: Map[String, String], headers: Map[String, String], body: Json): Json =
    Json.obj(
      "method" -> Json.fromString(method),
      "path" -> Json.fromString(path),
      "query" -> Json.fromFields(query.map { case (k, v) => k -> Json.fromString(v) }),
      "headers" -> Json.fromFields(headers.map { case (k, v) => k -> Json.fromString(v.toString) }),
      "body" -> body
    )

  /** The same for a message that a trigger read from a broker. */
  def message(source: String, key: Option[String], position: String, headers: Map[String, String], body: Json): Json =
    Json.obj(
      "source" -> Json.fromString(source),
      "key" -> key.fold(Json.Null)(Json.fromString),
      "position" -> Json.fromString(position),
      "headers" -> Json.fromFields(headers.map { case (k, v) => k -> Json.fromString(v) }),
      "body" -> body
    )

  /** The inputs of the flow. See [[TriggerDef.inputs]]. An input whose reference points to something that is not in the
    * request is left out, and the engine says which input is missing.
    */
  def inputs(trigger: TriggerDef, request: Json, env: EnvRefs.Env = EnvRefs.system): Json = {
    // The older form: $body.x, $query.x... A reference starts with one of the fields of the request.
    val Roots = request.asObject.fold(Set.empty[String])(_.keys.toSet)
    val lookup = Refs.lookup(
      trigger.origin -> Refs.at(request),
      "env" -> (path => env(path.mkString(".")).filter(_.nonEmpty).map(Json.fromString))
    )
    if (trigger.inputs.isEmpty) request.hcursor.downField("body").focus.filter(_.isObject).getOrElse(Json.obj())
    else
      Json.fromFields(trigger.inputs.toList.flatMap { case (name, value) =>
        value.asString match {
          case Some(t) if t.startsWith("$") && !t.startsWith("${") && !t.startsWith("$${") =>
            val segments = t.drop(1).split("\\.", -1).toList
            if (segments.headOption.exists(Roots) && segments.forall(_.nonEmpty)) StatePath(segments).get(request).map(name -> _).toList
            else List(name -> value)
          case _ => Refs.resolve(value, lookup).toOption.map(name -> _).toList
        }
      })
  }
}
