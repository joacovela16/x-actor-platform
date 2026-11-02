package xplat.core.connectors

import cats.Functor
import cats.implicits._
import xplat.core.api.{ActorRef, Defaults, ReplyTo}

import scala.concurrent.duration.FiniteDuration

/** Where a consumer delivers each message. The message is acknowledged when `deliver` completes; if it fails, it is
  * retried as the [[RetryPolicy]] says.
  */
trait Sink[F[_], T] {
  def deliver(message: Incoming[T]): F[Unit]
}

object Sink {

  def apply[F[_], T](f: Incoming[T] => F[Unit]): Sink[F, T] = (message: Incoming[T]) => f(message)

  /** Asks a local actor and waits for its [[Done]]. For a sharded entity, build the sink with `entityRef.ask`:
    * {{{
    * Sink[IO, Order](m => sharding.entityRefFor(OrderKey, m.key.get).ask[Done](Process(m.value, _)).void)
    * }}}
    */
  def actor[F[_]: Functor, T, M](ref: ActorRef[F, M], timeout: FiniteDuration = Defaults.ASK_TIMEOUT)(wrap: (Incoming[T], ReplyTo[Done]) => M): Sink[F, T] =
    (message: Incoming[T]) => ref.ask[Done](replyTo => wrap(message, replyTo), timeout).void
}
