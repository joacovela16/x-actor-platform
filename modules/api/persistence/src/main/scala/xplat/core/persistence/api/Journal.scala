package xplat.core.persistence.api

import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}

trait Journal[F[_]] {

  def create(shard: Shard): F[Unit]

  def getEvents(shard: Shard): F[fs2.Stream[F, JournalEvent]]

  def getSnapshot(shard: Shard): F[Option[JournalSnapshot]]

  def writeSnapshot(shard: Shard, data: Array[Byte]): F[Unit]
/*
  def deleteEventsByRange(shard: Shard, from: Long, to: Long): F[Unit]

  def getOffset(shard: Shard): F[Long]*/

  def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): F[Unit]

  def close(): F[Unit]
}

object Journal {

}
