package xplat.core.io.sharding

import cats.effect.{IO, Ref}
import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}
import xplat.core.persistence.api.{Journal, JournalBuilder}

/** Journal kept in memory and shared by every node, standing in for a database. As the real ones, after a snapshot
  * only newer events are read back.
  */
final class SharedJournal(state: Ref[IO, Map[Shard, SharedJournal.Entry]]) extends Journal[IO] {
  import SharedJournal.Entry

  private def entry(shard: Shard): IO[Entry] = state.get.map(_.getOrElse(shard, Entry(Vector.empty, None)))

  def create(shard: Shard): IO[Unit] = IO.unit

  def getEvents(shard: Shard): IO[fs2.Stream[IO, JournalEvent]] =
    entry(shard).map { e =>
      val from = e.snapshot.fold(0L)(_._1)
      fs2.Stream.emits(e.events.zipWithIndex.collect { case (d, i) if i + 1 > from => JournalEvent(shard.name, shard.id, i.toLong + 1, 0L, d) })
    }

  def getSnapshot(shard: Shard): IO[Option[JournalSnapshot]] =
    entry(shard).map(e => e.snapshot.map { case (offset, data) => JournalSnapshot(shard.name, shard.id, e.events.size.toLong, offset, Some(data)) })

  def writeSnapshot(shard: Shard, data: Array[Byte]): IO[Unit] =
    state.update { m =>
      val e = m.getOrElse(shard, Entry(Vector.empty, None))
      m.updated(shard, e.copy(snapshot = Some(e.events.size.toLong -> data)))
    }

  def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): IO[Unit] =
    state.update { m =>
      val e = m.getOrElse(shard, Entry(Vector.empty, None))
      m.updated(shard, e.copy(events = e.events ++ entries))
    }

  def close(): IO[Unit] = IO.unit

  /** Whether the shard has a snapshot. */
  def hasSnapshot(shard: Shard): IO[Boolean] = entry(shard).map(_.snapshot.isDefined)

  def builder: JournalBuilder[IO] = () => IO.pure(this)
}

object SharedJournal {
  final case class Entry(events: Vector[Array[Byte]], snapshot: Option[(Long, Array[Byte])])

  def apply(): IO[SharedJournal] = Ref.of[IO, Map[Shard, Entry]](Map.empty).map(new SharedJournal(_))
}
