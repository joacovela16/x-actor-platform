package xplat.core.connectors

import cats.effect.kernel.{Outcome, Resource, Temporal}
import cats.implicits._
import org.typelevel.log4cats.Logger
import xplat.core.api.Codec

import scala.concurrent.duration.FiniteDuration

/** Building blocks shared by the connector implementations. */
object Consuming {

  /** A message could not be decoded with the codec of the consumer. */
  final case class DecodeError(source: String, position: String, reason: String)
      extends RuntimeException(s"cannot decode the message at $position of $source: $reason")

  /** The consumer stopped because a message could not be processed (see [[RetryPolicy.whenExhausted]]). It is not
    * acknowledged, so the next consumer of the same group or queue receives it again.
    */
  final case class ConsumerStopped(source: String, position: String, cause: Throwable)
      extends RuntimeException(s"stopped at the message $position of $source: ${cause.getMessage}", cause)

  def decode[T](codec: Codec[T, Array[Byte]], source: String, position: String, bytes: Array[Byte]): Either[Throwable, T] =
    codec.decode(bytes).leftMap(e => DecodeError(source, position, e.message))

  /** Delivers one message following the policy. Completes when it can be acknowledged (processed or skipped); fails with
    * [[ConsumerStopped]] when the consumer must stop without acknowledging it.
    */
  def deliver[F[_], T](sink: Sink[F, T], policy: RetryPolicy, log: Logger[F])(source: String, position: String, message: Either[Throwable, Incoming[T]])(implicit
      T: Temporal[F]
  ): F[Unit] = {
    def exhausted(error: Throwable): F[Unit] = policy.whenExhausted match {
      case RetryPolicy.Skip => log.error(error)(s"skipping the message $position of $source")
      case RetryPolicy.Stop => T.raiseError(ConsumerStopped(source, position, error))
    }

    def attempt(incoming: Incoming[T], retry: Int, backoff: FiniteDuration): F[Unit] =
      sink.deliver(incoming).handleErrorWith { error =>
        if (retry >= policy.maxRetries) exhausted(error)
        else
          log.warn(error)(s"message $position of $source failed, retrying in $backoff (${retry + 1}/${policy.maxRetries})") >>
            T.sleep(backoff) >> attempt(incoming, retry + 1, (backoff * 2).min(policy.maxBackoff))
      }

    message match {
      case Left(error)     => exhausted(error)
      case Right(incoming) => attempt(incoming, 0, policy.initialBackoff)
    }
  }

  /** Runs the consumer loop in the background while the resource is in use. */
  def background[F[_]](loop: F[Unit])(implicit T: Temporal[F]): Resource[F, RunningConsumer[F]] =
    T.background(loop).map { outcome =>
      new RunningConsumer[F] {
        def stopped: F[Unit] = outcome.flatMap {
          case Outcome.Succeeded(_) => T.unit
          case Outcome.Errored(e)   => T.raiseError(e)
          case Outcome.Canceled()   => T.never
        }
      }
    }
}
