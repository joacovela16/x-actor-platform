package xplat.core.io.sharding

import cats.effect.{IO, Ref}
import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}
import xplat.core.persistence.api.{Journal, JournalBuilder}

/** Journal kept in memory and shared by every node, standing in for a database. Snapshots are not kept, so recovery
  * replays every event.
  */
final class SharedJournal(events: Ref[IO, Map[Shard, Vector[Array[Byte]]]]) extends Journal[IO] {
  def create(shard: Shard): IO[Unit] = IO.unit
  def getEvents(shard: Shard): IO[fs2.Stream[IO, JournalEvent]] =
    events.get.map(m => fs2.Stream.emits(m.getOrElse(shard, Vector.empty).zipWithIndex.map { case (d, i) => JournalEvent(shard.name, shard.id, i.toLong + 1, 0L, d) }))
  def getSnapshot(shard: Shard): IO[Option[JournalSnapshot]] = IO.pure(None)
  def writeSnapshot(shard: Shard, data: Array[Byte]): IO[Unit] = IO.unit
  def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): IO[Unit] = events.update(m => m.updated(shard, m.getOrElse(shard, Vector.empty) ++ entries))
  def close(): IO[Unit] = IO.unit

  def builder: JournalBuilder[IO] = () => IO.pure(this)
}

object SharedJournal {
  def apply(): IO[SharedJournal] = Ref.of[IO, Map[Shard, Vector[Array[Byte]]]](Map.empty).map(new SharedJournal(_))
}
