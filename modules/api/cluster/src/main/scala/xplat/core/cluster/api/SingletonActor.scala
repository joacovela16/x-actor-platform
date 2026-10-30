package xplat.core.cluster.api

import xplat.core.api.{Behavior, Behaviors, Codec}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** @param role
  *   if set, the singleton only runs on nodes with this role.
  * @param bufferSize
  *   messages kept while the singleton moves to another node. Beyond it, the oldest messages are dropped.
  * @param handOverRetryInterval
  *   how often the new node asks the old one to hand over the singleton.
  */
final case class SingletonConfig(
    role: Option[String] = None,
    bufferSize: Int = 1000,
    handOverRetryInterval: FiniteDuration = 1.second
)

/** Definition of an actor that runs on exactly one node of the cluster: the oldest member (with the role, if set). When
  * that node leaves or is downed, the singleton starts on the next oldest one.
  *
  * @param behavior
  *   same shape as `Persistence.build`, so a persistent singleton keeps its state when it moves.
  * @param stopMessage
  *   message sent to stop the singleton before it moves, so it can finish its work. `None` stops it directly.
  * @param codec
  *   encodes messages sent to the singleton from other nodes.
  */
final case class SingletonActor[F[_], T](
    name: String,
    behavior: Behaviors[F, T] => Behavior[F, T],
    config: SingletonConfig = SingletonConfig(),
    stopMessage: Option[T] = None
)(implicit val codec: Codec[T, Array[Byte]]) {
  def withConfig(config: SingletonConfig): SingletonActor[F, T] = copy(config = config)
  def withStopMessage(message: T): SingletonActor[F, T] = copy(stopMessage = Some(message))
}

object SingletonActor {
  def apply[F[_], T](name: String)(behavior: Behaviors[F, T] => Behavior[F, T])(implicit codec: Codec[T, Array[Byte]]): SingletonActor[F, T] =
    new SingletonActor[F, T](name, behavior)
}
