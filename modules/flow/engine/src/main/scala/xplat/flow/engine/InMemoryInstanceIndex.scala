package xplat.flow.engine

import cats.effect.{Ref, Sync}
import cats.syntax.functor._

/** An [[InstanceIndex]] in memory: it knows the instances that this process has run or brought up. */
final class InMemoryInstanceIndex[F[_]: Sync] private (all: Ref[F, Map[InstanceId, InstanceSummary]]) extends InstanceIndex[F] {

  def update(summary: InstanceSummary): F[Unit] = all.update(_.updated(summary.id, summary))

  def search(q: InstanceQuery): F[Page[InstanceSummary]] = all.get.map { instances =>
    val ordered = instances.values.toList
      .filter(i => q.flowId.forall(_ == i.flowId) && q.status.forall(_ == i.status) && q.origin.forall(i.origin.label.startsWith))
      .filter(i => q.from.forall(i.startedAt >= _) && q.to.forall(i.startedAt < _))
      .sortBy(i => (-i.startedAt, i.id))
    // Go on after the instance that the last page ended with; if it is not there, from the start.
    val rest = q.after.fold(ordered) { id =>
      val position = ordered.indexWhere(_.id == id)
      if (position < 0) ordered else ordered.drop(position + 1)
    }
    val limit = math.max(1, q.limit)
    val items = rest.take(limit)
    Page(items, if (rest.size > limit) items.lastOption.map(_.id) else None)
  }
}

object InMemoryInstanceIndex {

  def make[F[_]: Sync]: F[InMemoryInstanceIndex[F]] = Ref.of[F, Map[InstanceId, InstanceSummary]](Map.empty).map(new InMemoryInstanceIndex[F](_))
}
