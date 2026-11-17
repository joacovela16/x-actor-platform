package xplat.flow.http

import io.circe.{Json, JsonObject}
import xplat.flow.model.{FlowDef, NodeCatalog, NodeDef}

/** The secret fields of the configuration of the nodes (`secret` in their schema) do not go to the UI. When a flow is read they
  * are removed; when it is saved a node that does not bring one keeps the value it had.
  */
object FlowSecrets {

  private[http] def secretFields(catalog: NodeCatalog, node: NodeDef): Set[String] =
    catalog.get(node.kind).toList.flatMap(_.config.fields).filter(_.secret).map(_.name).toSet

  /** The flow without the secret fields of its nodes. */
  def strip(flow: FlowDef, catalog: NodeCatalog): FlowDef =
    flow.copy(nodes = flow.nodes.map { node =>
      val secret = secretFields(catalog, node)
      if (secret.isEmpty) node else node.copy(params = node.params.mapObject(_.filterKeys(!secret(_))))
    })

  /** The secret fields that the new flow does not bring are taken from the node with the same id in `previous`. */
  def keep(flow: FlowDef, previous: Option[FlowDef], catalog: NodeCatalog): FlowDef = {
    val before = previous.fold(Map.empty[String, NodeDef])(_.nodes.map(n => n.id -> n).toMap)
    flow.copy(nodes = flow.nodes.map { node =>
      val secret = secretFields(catalog, node)
      before.get(node.id).filter(_.kind == node.kind) match {
        case Some(old) if secret.nonEmpty =>
          val kept = secret.toList.flatMap(f => old.params.asObject.flatMap(_(f)).map(f -> _)).filter { case (f, _) => node.params.asObject.forall(!_.contains(f)) }
          if (kept.isEmpty) node
          else node.copy(params = Json.fromJsonObject(node.params.asObject.getOrElse(JsonObject.empty).deepMerge(JsonObject.fromIterable(kept))))
        case _ => node
      }
    })
  }
}
