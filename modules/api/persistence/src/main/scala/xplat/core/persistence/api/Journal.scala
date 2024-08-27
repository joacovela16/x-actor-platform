package xplat.core.persistence.api

import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot}

trait Journal[F[_]] {

  def create(shard: Journal.Shard): F[Unit]

  def getEvents(shard: Journal.Shard): F[LazyList[JournalEvent]]

  def getSnapshot(shard: Journal.Shard): F[Option[JournalSnapshot]]

  def writeSnapshot(shard: Journal.Shard, data: Array[Byte]): F[Unit]

  def getEventsByRange(shard: Journal.Shard, from: Long, to: Long): F[LazyList[JournalEvent]]

  def deleteEventsByRange(shard: Journal.Shard, from: Long, to: Long): F[Unit]

  def getOffset(shard: Journal.Shard): F[Long]

  def writeEvents(shard: Journal.Shard, entries: Seq[Array[Byte]]): F[Unit]

  def close(): F[Unit]
}

object Journal {

  /** represents a namespace "name" containing "id" */
  final case class Shard(name: String, id: String)
}
