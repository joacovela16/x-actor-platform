package xplat.core.sharding.delivery

import xplat.core.api.Ref

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** A message from a [[ReliableProducer]] to a sharded entity. The entity calls [[confirm]] once it has processed it;
  * until then the producer keeps sending it again.
  *
  * @param producerId
  *   who sent it; sequence numbers are per producer.
  * @param seq
  *   position of the message among those of the producer. The messages for one entity arrive in increasing order.
  */
final case class Delivery[T](producerId: String, seq: Long, message: T, confirmTo: Ref[Long]) {
  def confirm(): Unit = confirmTo ! seq
}

/** Sends messages to sharded entities so that they are not lost when processes stop.
  *
  * Each message is stored before [[send]] returns, and delivered to its entity again and again until the entity
  * confirms it, also after the producer restarts: a new producer with the same id resumes the unconfirmed messages.
  * Messages for one entity are delivered one at a time and in order.
  *
  * Delivery is at least once: a message may arrive again if a confirmation is lost. Entities drop those duplicates with
  * a [[Deduplicator]], or with [[DeliveryProgress]] in their persistent state. Only one producer with a given id may run
  * at a time.
  */
trait ReliableProducer[F[_], T] {

  /** Stores the message and schedules its delivery. Returns its sequence number. */
  def send(entityId: String, message: T): F[Long]

  /** Messages stored but not yet confirmed. */
  def unconfirmed: F[Int]
}

/** @param deliveryTimeout
  *   how long to wait for the confirmation of one attempt.
  * @param retryInterval
  *   pause after a failed attempt.
  * @param snapshotEvery
  *   confirmations between snapshots of the unconfirmed messages, so a restart reads few events.
  */
final case class ReliableProducerSettings(deliveryTimeout: FiniteDuration = 3.seconds, retryInterval: FiniteDuration = 1.second, snapshotEvery: Int = 100) {
  require(snapshotEvery > 0, "snapshotEvery must be positive")
}

/** Consumer side: processes each delivery once and confirms it, dropping (but confirming again) the ones already
  * processed.
  */
trait Deduplicator[F[_]] {

  /** Runs `process` if the delivery is new, then confirms it. If `process` fails, nothing is confirmed and the producer
    * sends it again.
    */
  def handle[T](delivery: Delivery[T])(process: T => F[Unit]): F[Unit]
}

/** Last sequence number processed from each producer. Persistent entities keep it in their state and update it with the
  * same event that applies the message, so a message is applied exactly once even across restarts.
  */
final case class DeliveryProgress(processed: Map[String, Long] = Map.empty) {
  def isNew(delivery: Delivery[_]): Boolean = processed.get(delivery.producerId).forall(delivery.seq > _)

  def record(producerId: String, seq: Long): DeliveryProgress =
    if (processed.get(producerId).exists(_ >= seq)) this else DeliveryProgress(processed.updated(producerId, seq))

  def record(delivery: Delivery[_]): DeliveryProgress = record(delivery.producerId, delivery.seq)
}
