package xplat.flow.model

import io.circe.{Decoder, Encoder, Json}

/** What a node returns when it finishes.
  *
  * @param values
  *   what it writes in the state, by the name of its [[OutputDecl]]. The engine puts each one at its target and checks
  *   its type.
  * @param port
  *   the port the execution goes on through.
  * @param memory
  *   the new memory of the node, if it changes.
  * @param error
  *   why it failed, when it did. A node that fails leaves through [[Port.Error]].
  * @param info
  *   facts about this run that are not state, for the audit trail (a node that starts another flow says which instance).
  * @param suspendUntil
  *   the answer is held until this time (milliseconds since the epoch): the node stays running, the wait is recorded, and when the
  *   time comes the engine finishes it with `values`, `port`, `memory` and `info`, without running it again. It survives a restart.
  *   A time that is not in the future makes no wait.
  */
final case class NodeOutput(
    values: Map[String, Json] = Map.empty,
    port: String = Port.Out,
    memory: Option[Json] = None,
    error: Option[String] = None,
    info: Map[String, Json] = Map.empty,
    suspendUntil: Option[Long] = None
) {
  def failed: Boolean = error.isDefined
}

object NodeOutput {

  def ok(values: Map[String, Json] = Map.empty): NodeOutput = NodeOutput(values)

  /** Finishes without writing anything, through `port`. */
  def via(port: String): NodeOutput = NodeOutput(port = port)

  def failed(reason: String): NodeOutput = NodeOutput(port = Port.Error, error = Some(reason))

  /** Waits until `time`, and then goes on through `out`. */
  def waitUntil(time: Long, info: Map[String, Json] = Map.empty): NodeOutput = NodeOutput(info = info, suspendUntil = Some(time))

  implicit val encoder: Encoder[NodeOutput] =
    Encoder
      .forProduct6("values", "port", "memory", "error", "info", "suspendUntil")((o: NodeOutput) => (o.values, o.port, o.memory, o.error, o.info, o.suspendUntil))
      .mapJsonObject(_.filter { case (_, v) => !v.isNull })

  implicit val decoder: Decoder[NodeOutput] = Decoder.instance { c =>
    for {
      values <- c.getOrElse[Map[String, Json]]("values")(Map.empty)
      port <- c.getOrElse[String]("port")(Port.Out)
      memory <- c.get[Option[Json]]("memory")
      error <- c.get[Option[String]]("error")
      info <- c.getOrElse[Map[String, Json]]("info")(Map.empty)
      suspendUntil <- c.get[Option[Long]]("suspendUntil")
    } yield NodeOutput(values, port, memory, error, info, suspendUntil)
  }
}
