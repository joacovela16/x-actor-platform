package xplat.core.sharding.api

/** Given to the behavior of a sharded entity when it starts. */
trait EntityContext[F[_], T] {
  def typeKey: EntityTypeKey[T]
  def entityId: String
  def shardId: String

  /** Asks the shard to stop this entity. Messages that arrive meanwhile are buffered and start it again afterwards, so
    * no message is lost. Prefer it over stopping the actor directly.
    */
  def passivate: F[Unit]
}
