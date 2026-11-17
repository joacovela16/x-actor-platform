package xplat.flow.plugin

import io.circe.{Encoder, Json}

/** What became of a plugin found in the plugins directory, for the logs and the admin view.
  *
  * @param dir
  *   its directory, relative to the plugins directory.
  * @param manifest
  *   absent when the manifest could not be read.
  * @param builtin
  *   whether it comes with the server (then `dir` is its name).
  */
final case class PluginStatus(dir: String, manifest: Option[PluginManifest], state: PluginStatus.State, builtin: Boolean = false)

object PluginStatus {

  sealed trait State
  object State {

    /** @param kinds the full names of its kinds of node; `connectionKinds`, of connection; `entities`, of the entities it brings;
      * `triggers`, the kinds of trigger it serves.
      */
    final case class Loaded(kinds: List[String], connectionKinds: List[String], entities: List[String] = Nil, triggers: List[String] = Nil) extends State
    final case class Failed(reason: String) extends State
  }

  implicit val encoder: Encoder[PluginStatus] = Encoder.instance { s =>
    val state = s.state match {
      case State.Loaded(kinds, conns, entities, triggers) =>
        Json.obj(
          "state" -> Json.fromString("loaded"),
          "kinds" -> Json.fromValues(kinds.map(Json.fromString)),
          "connectionKinds" -> Json.fromValues(conns.map(Json.fromString)),
          "entities" -> Json.fromValues(entities.map(Json.fromString)),
          "triggers" -> Json.fromValues(triggers.map(Json.fromString))
        )
      case State.Failed(reason) => Json.obj("state" -> Json.fromString("failed"), "error" -> Json.fromString(reason))
    }
    Json.obj("dir" -> Json.fromString(s.dir), "source" -> Json.fromString(if (s.builtin) "builtin" else "directory"), "manifest" -> s.manifest.fold(Json.Null)(Encoder[PluginManifest].apply(_))).deepMerge(state)
  }
}
