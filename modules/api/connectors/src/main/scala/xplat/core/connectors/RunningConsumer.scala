package xplat.core.connectors

/** A consumer running in the background. Releasing its resource stops it; messages being processed are not
  * acknowledged and arrive again later.
  */
trait RunningConsumer[F[_]] {

  /** Completes when the consumer stops on its own: with [[Consuming.ConsumerStopped]] if a message could not be
    * processed, or with the broker error. While it runs normally it does not complete.
    */
  def stopped: F[Unit]
}
