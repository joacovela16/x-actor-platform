package xplat.flow.engine

import cats.effect.{Ref, Sync}
import cats.syntax.functor._
import xplat.core.persistence.api.Journal
import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}

/** A journal that keeps everything in memory: for tests and for running without a database. Closing it does nothing, so
  * one journal can be shared by all the instances, and by several engines to see an instance recovered.
  */
final class InMemoryJournal[F[_]: Sync] private (data: Ref[F, InMemoryJournal.Data]) extends Journal[F] {

  def create(shard: Shard): F[Unit] = Sync[F].unit

  def getEvents(shard: Shard): F[fs2.Stream[F, JournalEvent]] = data.get.map { d =>
    val offset = d.snapshotOffset.getOrElse(shard, 0)
    fs2.Stream.emits(d.events.getOrElse(shard, Vector.empty).zipWithIndex.drop(offset).map { case (bytes, i) =>
      JournalEvent(shard.name, shard.id, i.toLong, 0L, bytes)
    })
  }

  def getSnapshot(shard: Shard): F[Option[JournalSnapshot]] = data.get.map { d =>
    val offset = d.snapshotOffset.getOrElse(shard, 0)
    d.snapshots.get(shard).map(bytes => JournalSnapshot(shard.name, shard.id, offset.toLong, offset.toLong, Some(bytes)))
  }

  def writeSnapshot(shard: Shard, bytes: Array[Byte]): F[Unit] = data.update { d =>
    d.copy(snapshots = d.snapshots.updated(shard, bytes), snapshotOffset = d.snapshotOffset.updated(shard, d.events.getOrElse(shard, Vector.empty).size))
  }

  def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): F[Unit] =
    data.update(d => d.copy(events = d.events.updated(shard, d.events.getOrElse(shard, Vector.empty) ++ entries)))

  def close(): F[Unit] = Sync[F].unit
}

object InMemoryJournal {

  private final case class Data(events: Map[Shard, Vector[Array[Byte]]], snapshots: Map[Shard, Array[Byte]], snapshotOffset: Map[Shard, Int])

  def make[F[_]: Sync]: F[InMemoryJournal[F]] = Ref.of[F, Data](Data(Map.empty, Map.empty, Map.empty)).map(new InMemoryJournal[F](_))
}
