package xplat.core.io.sharding.delivery

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{Mutex, Supervisor}
import cats.implicits._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.error.CodecError
import xplat.core.api.Codec
import xplat.core.cluster.api.RefResolver
import xplat.core.io.net.{BinReader, BinWriter}
import xplat.core.persistence.api.journal.Shard
import xplat.core.persistence.api.{Journal, JournalBuilder}
import xplat.core.sharding.ClusterSharding
import xplat.core.sharding.api.EntityTypeKey
import xplat.core.sharding.delivery._

import scala.collection.immutable.{Queue, SortedMap}
import scala.util.Try

object DeliveryCodec {

  /** Codec of the confirmations: the sequence number. */
  val confirmation: Codec[Long, Array[Byte]] = new Codec[Long, Array[Byte]] {
    def encode(from: Long): Either[CodecError, Array[Byte]] = Right(BinWriter().long(from).result())
    def decode(to: Array[Byte]): Either[CodecError, Long] = Try(BinReader(to).long()).toEither.leftMap(e => CodecError(s"bad confirmation: $e"))
  }

  /** Codec for [[Delivery]], to embed in the codec of the entity's protocol. Build it with the node's resolver. */
  def apply[F[_], T](resolver: RefResolver[F])(implicit message: Codec[T, Array[Byte]]): Codec[Delivery[T], Array[Byte]] =
    new Codec[Delivery[T], Array[Byte]] {
      def encode(from: Delivery[T]): Either[CodecError, Array[Byte]] =
        for {
          payload <- message.encode(from.message)
          ref     <- Try(resolver.serialize(from.confirmTo)).toEither.leftMap(e => CodecError(s"cannot send the confirmation ref: $e"))
        } yield BinWriter().string(from.producerId).long(from.seq).bytes(payload).string(ref).result()

      def decode(to: Array[Byte]): Either[CodecError, Delivery[T]] =
        Try {
          val r = BinReader(to)
          (r.string(), r.long(), r.bytes(), r.string())
        }.toEither.leftMap(e => CodecError(s"bad delivery: $e")).flatMap { case (producer, seq, payload, ref) =>
          message.decode(payload).flatMap { msg =>
            Try(Delivery(producer, seq, msg, resolver.resolve[Long](ref)(confirmation))).toEither.leftMap(e => CodecError(s"bad confirmation ref: $e"))
          }
        }
    }
}

/** [[ReliableProducer]] that stores its messages in a journal. */
object IOReliableProducer {

  /** Starts the producer `producerId`, resuming the messages it had not delivered. Releasing it stops the deliveries;
    * the unconfirmed messages stay stored.
    *
    * @param wrap
    *   puts a delivery in the entity's protocol.
    */
  def resource[F[_], T, M](
      producerId: String,
      sharding: ClusterSharding[F],
      typeKey: EntityTypeKey[M],
      journals: JournalBuilder[F],
      settings: ReliableProducerSettings = ReliableProducerSettings()
  )(wrap: Delivery[T] => M)(implicit codec: Codec[T, Array[Byte]], A: Async[F]): Resource[F, ReliableProducer[F, T]] =
    for {
      log        <- Resource.eval(Slf4jLogger.fromName[F]("xplat.sharding.delivery"))
      journal    <- Resource.eval(journals.build())
      shard       = Shard("xplat-reliable-producer", producerId)
      initial    <- Resource.eval(load(journal, shard))
      state      <- Resource.eval(Ref.of[F, State](initial))
      writes     <- Resource.eval(Mutex[F])
      supervisor <- Supervisor[F](await = false)
      producer    = new Producer[F, T, M](producerId, sharding, typeKey, journal, shard, settings, wrap, state, writes, supervisor, log)
      _          <- Resource.eval(initial.queues.keys.toList.traverse_(producer.kick))
    } yield producer

  // ---- State and its events ----

  private[delivery] final case class State(nextSeq: Long, messages: SortedMap[Long, (String, Array[Byte])], queues: Map[String, Queue[Long]], inFlight: Set[String], sinceSnapshot: Int) {

    def stored(seq: Long, entityId: String, payload: Array[Byte]): State =
      copy(
        nextSeq = math.max(nextSeq, seq + 1),
        messages = messages.updated(seq, (entityId, payload)),
        queues = queues.updated(entityId, queues.getOrElse(entityId, Queue.empty[Long]).enqueue(seq))
      )

    def confirmed(seq: Long): State =
      messages.get(seq).fold(this) { case (entityId, _) =>
        val rest = queues.getOrElse(entityId, Queue.empty[Long]).filterNot(_ == seq)
        copy(messages = messages - seq, queues = if (rest.isEmpty) queues - entityId else queues.updated(entityId, rest))
      }
  }

  private[delivery] object State {
    val empty: State = State(1L, SortedMap.empty, Map.empty, Set.empty, 0)
  }

  private def load[F[_]](journal: Journal[F], shard: Shard)(implicit A: Async[F]): F[State] =
    for {
      _        <- journal.create(shard)
      snapshot <- journal.getSnapshot(shard)
      initial  <- A.delay(snapshot.flatMap(_.data).fold(State.empty)(decodeSnapshot))
      events   <- journal.getEvents(shard)
      state    <- events.compile.fold(initial)((s, e) => applyEvent(s, e.data))
    } yield state

  private def applyEvent(s: State, bytes: Array[Byte]): State = {
    val r = BinReader(bytes)
    r.byte() match {
      case 1     => s.stored(r.long(), r.string(), r.bytes())
      case 2     => s.confirmed(r.long())
      case other => throw new IllegalStateException(s"unknown producer event $other")
    }
  }

  private def storedEvent(seq: Long, entityId: String, payload: Array[Byte]): Array[Byte] =
    BinWriter().byte(1).long(seq).string(entityId).bytes(payload).result()

  private def confirmedEvent(seq: Long): Array[Byte] = BinWriter().byte(2).long(seq).result()

  private def encodeSnapshot(s: State): Array[Byte] =
    BinWriter().long(s.nextSeq).seq(s.messages) { case (w, (seq, (entityId, payload))) => w.long(seq).string(entityId).bytes(payload); () }.result()

  private def decodeSnapshot(bytes: Array[Byte]): State = {
    val r = BinReader(bytes)
    val next = r.long()
    r.seq(r => (r.long(), r.string(), r.bytes())).foldLeft(State.empty.copy(nextSeq = next)) { case (s, (seq, e, p)) => s.stored(seq, e, p) }
  }

  private final class Producer[F[_], T, M](
      producerId: String,
      sharding: ClusterSharding[F],
      typeKey: EntityTypeKey[M],
      journal: Journal[F],
      shard: Shard,
      settings: ReliableProducerSettings,
      wrap: Delivery[T] => M,
      state: Ref[F, State],
      writes: Mutex[F],
      supervisor: Supervisor[F],
      log: Log4Cats[F]
  )(implicit codec: Codec[T, Array[Byte]], A: Async[F])
      extends ReliableProducer[F, T] {

    def send(entityId: String, message: T): F[Long] =
      for {
        payload <- A.fromEither(codec.encode(message).leftMap(e => new IllegalArgumentException(e.message)))
        seq <- writes.lock.surround {
          state.get.map(_.nextSeq).flatTap(seq => journal.writeEvents(shard, Seq(storedEvent(seq, entityId, payload))) >> state.update(_.stored(seq, entityId, payload)))
        }
        _ <- kick(entityId)
      } yield seq

    def unconfirmed: F[Int] = state.get.map(_.messages.size)

    /** Starts delivering to the entity unless it is already being done. */
    def kick(entityId: String): F[Unit] =
      state.modify(s => if (s.inFlight(entityId)) (s, false) else (s.copy(inFlight = s.inFlight + entityId), true)).ifM(supervisor.supervise(drain(entityId)).void, A.unit)

    /** Delivers the messages of one entity in order, until none is left. Seeing an empty queue and leaving happen in one
      * step, so a message stored meanwhile either is seen here or starts a new drain.
      */
    private def drain(entityId: String): F[Unit] =
      state.modify { s =>
        s.queues.get(entityId).flatMap(_.headOption) match {
          case None      => (s.copy(inFlight = s.inFlight - entityId), None)
          case Some(seq) => (s, Some(seq -> s.messages(seq)._2))
        }
      }.flatMap {
        case None => A.unit
        case Some((seq, payload)) =>
          deliver(entityId, seq, payload).flatMap {
            case true  => confirm(seq) >> drain(entityId)
            case false => A.sleep(settings.retryInterval) >> drain(entityId)
          }
      }

    private def deliver(entityId: String, seq: Long, payload: Array[Byte]): F[Boolean] =
      codec.decode(payload) match {
        case Left(e) => log.error(s"Dropping message $seq of $producerId for $entityId: cannot decode it (${e.message})").as(true)
        case Right(message) =>
          sharding
            .entityRefFor(typeKey, entityId)
            .ask[Long](ref => wrap(Delivery(producerId, seq, message, ref)), settings.deliveryTimeout)(DeliveryCodec.confirmation)
            .attempt
            .flatMap {
              case Right(confirmed) => A.pure(confirmed >= seq)
              case Left(e)          => log.debug(e)(s"Delivery $seq of $producerId to $entityId failed; retrying").as(false)
            }
      }

    private def confirm(seq: Long): F[Unit] =
      writes.lock.surround {
        journal.writeEvents(shard, Seq(confirmedEvent(seq))) >>
          state.modify { s =>
            val next = s.confirmed(seq)
            if (next.sinceSnapshot + 1 >= settings.snapshotEvery) (next.copy(sinceSnapshot = 0), Some(next))
            else (next.copy(sinceSnapshot = next.sinceSnapshot + 1), None)
          }.flatMap(_.traverse_(s => journal.writeSnapshot(shard, encodeSnapshot(s))))
      }
  }
}

/** [[Deduplicator]]s for entities. Create one when the entity starts. */
object Deduplicators {

  /** Remembers what was processed while the entity runs. Enough for entities whose state is lost anyway when they
    * stop; otherwise use [[journaled]] or [[DeliveryProgress]] in the persistent state.
    */
  def inMemory[F[_]](implicit A: Async[F]): F[Deduplicator[F]] =
    Ref.of[F, DeliveryProgress](DeliveryProgress()).map { progress =>
      new Deduplicator[F] {
        def handle[T](delivery: Delivery[T])(process: T => F[Unit]): F[Unit] =
          progress.get.map(_.isNew(delivery)).ifM(process(delivery.message) >> progress.update(_.record(delivery)), A.unit) >>
            A.delay(delivery.confirm())
      }
    }

  /** Stores what was processed in a journal, so duplicates are dropped also after the entity restarts. A message can
    * still be processed twice if the process stops between processing it and storing that it was.
    */
  def journaled[F[_]](journals: JournalBuilder[F], typeName: String, entityId: String, snapshotEvery: Int = 100)(implicit A: Async[F]): F[Deduplicator[F]] =
    for {
      journal  <- journals.build()
      shard     = Shard("xplat-reliable-consumer", s"$typeName/$entityId")
      _        <- journal.create(shard)
      snapshot <- journal.getSnapshot(shard)
      initial   = snapshot.flatMap(_.data).fold(DeliveryProgress())(decodeProgress)
      events   <- journal.getEvents(shard)
      loaded   <- events.compile.fold(initial) { (p, e) =>
        val r = BinReader(e.data)
        p.record(r.string(), r.long())
      }
      progress <- Ref.of[F, (DeliveryProgress, Int)]((loaded, 0))
    } yield new Deduplicator[F] {
      def handle[T](delivery: Delivery[T])(process: T => F[Unit]): F[Unit] =
        progress.get.map(_._1.isNew(delivery)).ifM(
          process(delivery.message) >>
            journal.writeEvents(shard, Seq(BinWriter().string(delivery.producerId).long(delivery.seq).result())) >>
            progress.modify { case (p, n) =>
              val next = p.record(delivery)
              if (n + 1 >= snapshotEvery) ((next, 0), Some(next)) else ((next, n + 1), None)
            }.flatMap(_.traverse_(p => journal.writeSnapshot(shard, encodeProgress(p)))),
          A.unit
        ) >> A.delay(delivery.confirm())
    }

  private def encodeProgress(p: DeliveryProgress): Array[Byte] =
    BinWriter().seq(p.processed) { case (w, (producer, seq)) => w.string(producer).long(seq); () }.result()

  private def decodeProgress(bytes: Array[Byte]): DeliveryProgress = {
    val r = BinReader(bytes)
    DeliveryProgress(r.seq(r => r.string() -> r.long()).toMap)
  }
}
