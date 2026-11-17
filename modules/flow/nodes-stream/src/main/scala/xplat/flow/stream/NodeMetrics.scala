package xplat.flow.stream

import cats.effect.{Ref, Sync}
import cats.syntax.functor._
import io.circe.Json

/** What one node of a segment counts about itself: elements in, out, dropped on purpose (a filter) and failed (dead letters), how long
  * elements take to get there, and a few samples of what went in and out, for a person to look at. It is a plain counter and
  * costs nothing per element beyond updating it.
  */
final class NodeMetrics[F[_]: Sync] private (state: Ref[F, NodeMetrics.Counts], sampleSize: Int, sink: Boolean) {

  def in(e: Elem[F]): F[Unit] = state.update(c => c.copy(in = c.in + 1, sampleIn = keep(c.sampleIn, e.value)))

  def out(e: Elem[F], now: Long): F[Unit] =
    state.update(c => c.copy(out = c.out + 1, latencySum = c.latencySum + math.max(0L, now - e.at), latencyN = c.latencyN + 1, sampleOut = keep(c.sampleOut, e.value)))

  def dropped: F[Unit] = state.update(c => c.copy(dropped = c.dropped + 1))

  def failed: F[Unit] = state.update(c => c.copy(failed = c.failed + 1))

  private def keep(sample: Vector[Json], v: Json): Vector[Json] = if (sample.size < sampleSize) sample :+ v else sample

  def snapshot: F[Json] = state.get.map(_.toJson(sink))
}

object NodeMetrics {

  private[stream] final case class Counts(
      in: Long = 0,
      out: Long = 0,
      dropped: Long = 0,
      failed: Long = 0,
      latencySum: Long = 0,
      latencyN: Long = 0,
      sampleIn: Vector[Json] = Vector.empty,
      sampleOut: Vector[Json] = Vector.empty
  ) {
    // A sink hands nothing on: what it delivered is what it received and did not drop or fail.
    def toJson(sink: Boolean): Json = Json.obj(
      "in" -> Json.fromLong(in),
      "out" -> Json.fromLong(if (sink) math.max(0L, in - dropped - failed) else out),
      "dropped" -> Json.fromLong(dropped),
      "failed" -> Json.fromLong(failed),
      "latencyMs" -> (if (latencyN == 0) Json.Null else Json.fromLong(latencySum / latencyN)),
      "sampleIn" -> Json.fromValues(sampleIn),
      "sampleOut" -> Json.fromValues(sampleOut)
    )
  }

  /** @param sink
    *   the node ends a stream: its `out` is what it delivered, which it does not count one by one.
    */
  def make[F[_]: Sync](sampleSize: Int = 5, sink: Boolean = false): F[NodeMetrics[F]] = Ref.of[F, Counts](Counts()).map(new NodeMetrics[F](_, sampleSize, sink))
}
