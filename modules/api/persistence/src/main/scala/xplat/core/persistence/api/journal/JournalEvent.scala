package xplat.core.persistence.api.journal

final case class JournalEvent(shard: String, id: String, offset: Long, timestamp: Long, data: Array[Byte])
