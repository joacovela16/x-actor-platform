package xplat.flow.nodes

import io.circe.Json

/** How a node finds the named connection that its configuration refers to (`kafka-prod`, `postgres-main`...). The module
  * that stores connections implements it, so the nodes depend on this small interface and not on the HTTP layer.
  */
trait Connections[F[_]] {

  /** The connection called `name`, with its secrets, or nothing if it does not exist. */
  def resolve(name: String): F[Option[Connections.Resolved]]
}

object Connections {

  /** @param params
    *   the configuration that is not secret.
    * @param secrets
    *   the secret fields, by name. They are only for the node that connects; they must never reach the audit or the UI.
    */
  final case class Resolved(name: String, kind: String, params: Json, secrets: Map[String, String]) {

    /** A text field of `params`. */
    def text(field: String): Option[String] = params.hcursor.get[String](field).toOption.filter(_.nonEmpty)
  }

  /** No connection exists. */
  def none[F[_]](pure: Option[Resolved] => F[Option[Resolved]]): Connections[F] = (_: String) => pure(None)
}
