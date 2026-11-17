package xplat.flow.nodes

import io.circe.Json
import xplat.flow.model.{NodeInput, Refs, StatePath}

/** A document of the configuration (a filter, an update, a pipeline...) with values taken from the run, with the references of
  * [[Refs]]: `"${state.order.id}"` (a value of the global state) and `"${context.key}"` (`instanceId`, `nodeId` or `key`, which is
  * `<instanceId>:<nodeId>`, for writes that a retry must not repeat). A text that is only a reference keeps the type of the value.
  *
  * The older markers still work: an object with only `{"$path": "order.id"}` or `{"$context": "key"}`.
  *
  * Everything else is kept as it is, so MongoDB Extended JSON works: `{"_id": {"$oid": "..."}}`, `{"at": {"$date": "..."}}`.
  * Values are put into the document as values, never as text of a query.
  */
object Template {

  val ContextNames: Set[String] = Set("instanceId", "nodeId", "key")

  /** The origins a node's configuration may use. */
  val Origins: Set[String] = Set("state", "context")

  private def lookup(input: NodeInput): Refs.Lookup =
    Refs.lookup(
      "state" -> Refs.at(input.state),
      "context" -> {
        case List(name) if ContextNames(name) => Some(Json.fromString(contextValue(name, input)))
        case _                                => None
      }
    )

  def resolve(template: Json, input: NodeInput): Either[String, Json] =
    template.fold(
      Right(template),
      _ => Right(template),
      _ => Right(template),
      t => Refs.resolveText(t, lookup(input)),
      values => values.foldLeft[Either[String, Vector[Json]]](Right(Vector.empty))((acc, v) => acc.flatMap(done => resolve(v, input).map(done :+ _))).map(Json.fromValues),
      obj =>
        marker(template) match {
          case Some(Left(problem))         => Left(problem)
          case Some(Right(Left(path)))     => path.get(input.state).toRight(s"'$path' is not in the state")
          case Some(Right(Right(context))) => Right(Json.fromString(contextValue(context, input)))
          case None =>
            obj.toList
              .foldLeft[Either[String, List[(String, Json)]]](Right(Nil))((acc, kv) => acc.flatMap(done => resolve(kv._2, input).map(v => done :+ (kv._1 -> v))))
              .map(Json.fromFields)
        }
    )

  /** A text of the configuration (a URL, a prompt...) with its references put in as text, each one through `escape`. */
  def text(t: String, input: NodeInput, escape: String => String = identity): Either[String, String] =
    Refs.resolveText(t, lookup(input), escape).map(j => j.asString.getOrElse(j.noSpaces))

  /** The paths of the state that a template reads (a reference with a default is not required, so it does not count). */
  def reads(template: Json): List[StatePath] =
    marker(template) match {
      case Some(Right(Left(path))) => List(path)
      case Some(_)                 => Nil
      case None =>
        template.asString
          .map(t => Refs.parse(t).getOrElse(Nil).collect { case r if r.origin == "state" && r.path.nonEmpty && r.default.isEmpty => StatePath(r.path) })
          .orElse(template.asArray.map(_.toList.flatMap(reads)))
          .orElse(template.asObject.map(_.values.toList.flatMap(reads)))
          .getOrElse(Nil)
          .distinct
    }

  def textReads(t: String): List[StatePath] = reads(Json.fromString(t))

  /** The problems of a template: broken references, origins a node cannot use, contexts that do not exist. */
  def problems(template: Json): List[String] =
    marker(template) match {
      case Some(Left(problem)) => List(problem)
      case Some(Right(_))      => Nil
      case None =>
        template.asString
          .map(textProblems)
          .orElse(template.asArray.map(_.toList.flatMap(problems)))
          .orElse(template.asObject.map(_.values.toList.flatMap(problems)))
          .getOrElse(Nil)
    }

  def textProblems(t: String): List[String] = {
    val refs = Refs.parse(t).getOrElse(Nil)
    Refs.problems(Json.fromString(t), Origins, "a node") ++ refs.flatMap { r =>
      if (r.origin == "context" && !(r.path.size == 1 && ContextNames(r.path.head)))
        Some(s"'$${${r.render}}': context is one of ${ContextNames.toList.sorted.mkString(", ")}")
      else if (r.origin == "state" && r.path.isEmpty) Some(s"'$${${r.render}}' needs a path, such as $${state.order.id}")
      else None
    }
  }

  private def contextValue(name: String, input: NodeInput): String = name match {
    case "instanceId" => input.instanceId
    case "nodeId"     => input.nodeId
    case _            => s"${input.instanceId}:${input.nodeId}"
  }

  /** `Some` when the value is one of the markers: a path (left) or a context (right), or why it is a broken one. */
  private def marker(json: Json): Option[Either[String, Either[StatePath, String]]] =
    json.asObject.filter(_.size == 1).flatMap { o =>
      o("$path")
        .map(p => p.as[StatePath].left.map(e => s"$$path: ${e.message}").map(Left(_)))
        .orElse(o("$context").map(n => n.asString.filter(ContextNames).map(Right(_)).toRight(s"$$context must be one of ${ContextNames.toList.sorted.mkString(", ")}")))
    }
}
