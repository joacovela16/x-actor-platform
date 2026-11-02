package xplat.core.sharding.api

import xplat.core.api.{Behavior, Behaviors, Codec, Mailbox}

/** Definition of a sharded entity type, given to [[xplat.core.sharding.ClusterSharding.init]].
  *
  * The entity actor is named after its entity id. A persistent entity built with `Persistence(name, ...)` therefore
  * stores its events under `Shard(name, entityId)`.
  *
  * @param behavior
  *   builds the behavior of one entity. It has the same shape as `Persistence.build`, so a persistent behavior can be used
  *   directly.
  * @param stopMessage
  *   message sent to an entity to stop it on passivation or hand-off, so it can finish its work first. `None` stops it
  *   directly.
  * @param allocationStrategy
  *   where shards are placed. `None` uses the implementation default.
  * @param mailbox
  *   mailbox of each entity. With [[xplat.core.api.Mailbox.Overflow.Fail]], a local send to a full entity fails with
  *   [[xplat.core.api.error.MailboxOverflow]]; a send from another node does not see that error, the message is lost.
  * @param codec
  *   encodes messages sent to entities on other nodes.
  */
final case class Entity[F[_], T](
    typeKey: EntityTypeKey[T],
    behavior: EntityContext[F, T] => Behaviors[F, T] => Behavior[F, T],
    config: ShardingConfig = ShardingConfig(),
    shardIdExtractor: ShardIdExtractor = ShardIdExtractor.hashCode(Entity.DefaultNumberOfShards),
    stopMessage: Option[T] = None,
    allocationStrategy: Option[ShardAllocationStrategy[F]] = None,
    mailbox: Mailbox = Mailbox.Unbounded
)(implicit val codec: Codec[T, Array[Byte]]) {
  def withConfig(config: ShardingConfig): Entity[F, T] = copy(config = config)
  def withShardIdExtractor(extractor: ShardIdExtractor): Entity[F, T] = copy(shardIdExtractor = extractor)
  def withStopMessage(message: T): Entity[F, T] = copy(stopMessage = Some(message))
  def withAllocationStrategy(strategy: ShardAllocationStrategy[F]): Entity[F, T] = copy(allocationStrategy = Some(strategy))
  def withMailbox(mailbox: Mailbox): Entity[F, T] = copy(mailbox = mailbox)
}

object Entity {
  val DefaultNumberOfShards: Int = 100

  def apply[F[_], T](typeKey: EntityTypeKey[T])(behavior: EntityContext[F, T] => Behaviors[F, T] => Behavior[F, T])(implicit codec: Codec[T, Array[Byte]]): Entity[F, T] =
    new Entity[F, T](typeKey, behavior)
}
