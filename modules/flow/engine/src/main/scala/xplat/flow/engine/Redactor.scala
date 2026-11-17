package xplat.flow.engine

import io.circe.Json
import xplat.flow.model.{NodeCatalog, NodeDef, StatePath, StreamSegments}

/** Hides secrets before they are written to the audit trail.
  *
  * Two things are hidden: the fields of the configuration of a node that its schema marks as `secret`, and anything in the
  * state at or under the `secretPaths`. Whatever is hidden shows as `{"$redacted": true}`.
  */
final case class Redactor(catalog: NodeCatalog, secretPaths: List[StatePath] = Nil) {

  /** The configuration of the node, without its secret fields. */
  def params(node: NodeDef): Json =
    if (node.kind == StreamSegments.SegmentKind)
      // A stream segment carries the nodes inside it: each one hides the secrets of its own kind.
      node.params.mapObject(_.add("members", Json.fromValues(StreamSegments.membersOf(node.params).map(m => io.circe.Encoder[NodeDef].apply(m.copy(params = params(m)))))))
    else {
      val secret = catalog.get(node.kind).toList.flatMap(_.config.fields).filter(_.secret).map(_.name).toSet
      if (secret.isEmpty) node.params
      else node.params.mapObject(o => io.circe.JsonObject.fromIterable(o.toIterable.map { case (k, v) => k -> (if (secret(k)) Redactor.Hidden else v) }))
    }

  /** The value found at `path` of the state, with what is secret in it hidden. */
  def value(path: StatePath, value: Json): Json =
    secretPaths.foldLeft(value) { (v, secret) =>
      if (path.startsWith(secret)) Redactor.Hidden
      else if (secret.startsWith(path)) {
        val inside = secret.dropPrefix(path)
        if (inside.get(v).isDefined) inside.set(v, Redactor.Hidden) else v
      } else v
    }

  def value(path: StatePath, value: Option[Json]): Option[Json] = value.map(this.value(path, _))

  /** The initial values of an instance, which are by the name of the inputs of the flow, with the secret ones hidden. */
  def inputs(flow: xplat.flow.model.FlowDef, inputs: Json): Json =
    flow.inputs.foldLeft(inputs) { (acc, decl) =>
      acc.hcursor.downField(decl.name).focus match {
        case Some(v) => acc.mapObject(_.add(decl.name, value(decl.target, v)))
        case None    => acc
      }
    }
}

object Redactor {
  /** What stands where a secret was. */
  val Hidden: Json = Json.obj("$redacted" -> Json.True)
}
