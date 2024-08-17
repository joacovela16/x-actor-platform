package xplat.core.persistence.api.journal

final case class JournalSnapshot(shard: String, id: String, offset: Long, snapshotOffset: Long, data: Option[Array[Byte]])
