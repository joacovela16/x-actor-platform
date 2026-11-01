package xplat.core.connectors

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** What to do when delivering a message fails.
  *
  * @param maxRetries
  *   attempts after the first one. Messages that cannot be decoded are not retried.
  * @param initialBackoff
  *   pause before the first retry; it doubles on each retry, up to `maxBackoff`.
  * @param whenExhausted
  *   [[RetryPolicy.Stop]] leaves the message unacknowledged and stops the consumer, so it is delivered again later;
  *   [[RetryPolicy.Skip]] acknowledges it and goes on with the next one.
  */
final case class RetryPolicy(
    maxRetries: Int = 5,
    initialBackoff: FiniteDuration = 500.millis,
    maxBackoff: FiniteDuration = 30.seconds,
    whenExhausted: RetryPolicy.WhenExhausted = RetryPolicy.Stop
) {
  require(maxRetries >= 0, "maxRetries must not be negative")
}

object RetryPolicy {
  sealed trait WhenExhausted
  case object Stop extends WhenExhausted
  case object Skip extends WhenExhausted
}
