package xplat.flow.nodes

import io.circe.Json

/** Something outside the server that starts flows, for the triggers of one kind on connections it can read (`queue` triggers on a
  * `kafka` connection). A plugin brings it in what it installs.
  *
  * The server keeps the triggers: for each one of [[kind]] whose connection is of a kind this source [[accepts]], it calls [[run]]
  * and keeps it running while the trigger and its connection stay the same. When they change, or the trigger is deleted, the run
  * is cancelled; when the run ends or fails, it is started again a few seconds later.
  */
trait TriggerSource[F[_]] {

  /** The kind of trigger it serves: `queue`. */
  def kind: String

  /** Whether it can read from a connection of this kind. When two sources accept the same trigger, the first one installed serves it. */
  def accepts(connectionKind: String): Boolean

  /** Reads `trigger` (its JSON, as `GET /api/triggers` shows it) from `connection` and calls `start` for each thing that should start
    * an instance, until it is cancelled. It ends only if the source stops on its own.
    */
  def run(trigger: Json, connection: Connections.Resolved, start: TriggerSource.Start[F]): F[Unit]
}

object TriggerSource {

  /** Starts an instance of the trigger's flow.
    *
    * @param request
    *   what arrived, which the `inputs` of the trigger map to the inputs of the flow (for a message: `source`, `key`, `position`,
    *   `headers`, `body`).
    * @param key
    *   what identifies it in the source (an idempotency key, or its place in the source): the same key does not start a second
    *   instance, so a message delivered again is harmless.
    * @param position
    *   where it was in the source, which the audit of the instance records.
    */
  trait Start[F[_]] {
    def apply(request: Json, key: String, position: Option[String]): F[Started]
  }

  sealed trait Started

  /** The instance started (or had started already with the same key). Acknowledge what arrived. */
  case object Accepted extends Started

  /** What arrived can never start the flow (its inputs do not fit): the server logged it. Acknowledge it too, or it comes back forever. */
  final case class Rejected(reason: String) extends Started
}
