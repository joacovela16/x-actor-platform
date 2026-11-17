package xplat.flow.engine

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.functor._
import io.circe.Json

/** What the running stream segments say about themselves, while they run. A segment reports a snapshot of its metrics from time
  * to time; the engine adds it to the status of the instance. When the segment ends, its final metrics go to the audit and the
  * snapshot is dropped. This is kept in the process: it is not recovered after a restart (the segment starts again then).
  *
  * A snapshot is `{"nodes": {nodeId: {"in", "out", "dropped", "failed", "latencyMs", "sampleIn", "sampleOut"}}, "deadLetters": [...]}`.
  */
trait SegmentMetrics[F[_]] {

  /** The latest snapshot of the segment of the instance that is anchored at `nodeId`. */
  def report(instanceId: String, nodeId: String, snapshot: Json): F[Unit]

  /** The snapshots of the segments that are running in the instance, by anchor. */
  def snapshot(instanceId: String): F[Map[String, Json]]

  def clear(instanceId: String, nodeId: String): F[Unit]
}

object SegmentMetrics {

  /** Metrics that are not kept anywhere. */
  def none[F[_]](implicit F: cats.Applicative[F]): SegmentMetrics[F] = new SegmentMetrics[F] {
    def report(instanceId: String, nodeId: String, snapshot: Json): F[Unit] = F.unit
    def snapshot(instanceId: String): F[Map[String, Json]] = F.pure(Map.empty)
    def clear(instanceId: String, nodeId: String): F[Unit] = F.unit
  }

  def inMemory[F[_]: Sync]: F[SegmentMetrics[F]] =
    Ref.of[F, Map[(String, String), Json]](Map.empty).map { ref =>
      new SegmentMetrics[F] {
        def report(instanceId: String, nodeId: String, snapshot: Json): F[Unit] = ref.update(_.updated((instanceId, nodeId), snapshot))
        def snapshot(instanceId: String): F[Map[String, Json]] = ref.get.map(_.collect { case ((i, n), j) if i == instanceId => n -> j })
        def clear(instanceId: String, nodeId: String): F[Unit] = ref.update(_ - ((instanceId, nodeId)))
      }
    }
}
