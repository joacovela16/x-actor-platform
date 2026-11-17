package xplat.flow.stream

import cats.effect.{Ref, Temporal}
import cats.syntax.all._
import io.circe.Json
import xplat.flow.model.{NodeDef, NodeInput}

import scala.concurrent.duration.FiniteDuration

/** What a node of a stream gets when its segment runs: its own definition, the metrics it keeps and the ways to say that an element
  * was dropped or failed.
  *
  * @param node
  *   the node, with its configuration (`params`) and the outputs it declared.
  * @param input
  *   what the segment received: the global state at the moment it started, and which instance it is.
  * @param simulated
  *   the segment is part of a simulation: nothing waits for real time, and what has effects outside is not done.
  */
final class StreamContext[F[_]](
    val node: NodeDef,
    val input: NodeInput,
    val simulated: Boolean,
    metrics: NodeMetrics[F],
    deadLetters: Ref[F, Vector[Json]],
    val now: F[Long],
    deadLetterLimit: Int
)(implicit val F: Temporal[F]) {

  def params: Json = node.params

  /** The element is not passed on, on purpose (a filter): it counts as dropped and is acknowledged. */
  def drop(e: Elem[F]): F[Unit] = metrics.dropped >> e.ack

  /** The element could not be handled: it goes to the dead letters with the reason, is counted and acknowledged, so a message that
    * will always fail does not come back for ever.
    */
  def fail(e: Elem[F], error: String): F[Unit] =
    for {
      at <- now
      letter = Json.obj("node" -> Json.fromString(node.id), "error" -> Json.fromString(error), "element" -> e.value, "at" -> Json.fromLong(at))
      _ <- deadLetters.update(d => if (d.size < deadLetterLimit) d :+ letter else d)
      _ <- metrics.failed
      _ <- e.ack
    } yield ()

  /** Waits, unless it is a simulation. */
  def sleep(d: FiniteDuration): F[Unit] = if (simulated) F.unit else F.sleep(d)

  /** The value that a sink hands to the state, under the first output it declared (none if it declared none). */
  def result(value: Json): Map[String, Json] = node.outputs.headOption.map(_.name -> value).toMap

  /** A new element made from `from`, with a value. */
  def element(from: Elem[F], value: Json): Elem[F] = from.withValue(value)
}
