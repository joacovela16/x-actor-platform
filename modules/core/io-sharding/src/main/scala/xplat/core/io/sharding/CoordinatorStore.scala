package xplat.core.io.sharding

import cats.effect.kernel.{Async, Ref}
import cats.implicits._
import xplat.core.cluster.api.Address
import xplat.core.io.net.{BinReader, BinWriter}
import xplat.core.persistence.api.journal.Shard
import xplat.core.persistence.api.{Journal, JournalBuilder}

/** Where the coordinator keeps its state, so it survives the coordinator moving or the whole cluster restarting. */
private[sharding] trait CoordinatorStore[F[_]] {
  def load: F[CoordinatorStore.Data]
  def save(before: CoordinatorStore.Data, after: CoordinatorStore.Data): F[Unit]
}

private[sharding] object CoordinatorStore {

  /** What is worth keeping: where each shard lives and which entities to restart. */
  final case class Data(allocations: Map[String, Address], remembered: Map[String, Set[String]])

  object Data {
    val empty: Data = Data(Map.empty, Map.empty)
  }

  /** Keeps nothing: the coordinator rebuilds its state from what the regions report. */
  def inMemory[F[_]](implicit A: Async[F]): CoordinatorStore[F] = new CoordinatorStore[F] {
    def load: F[Data] = A.pure(Data.empty)
    def save(before: Data, after: Data): F[Unit] = A.unit
  }

  /** Event-sourced on a [[Journal]]: changes are appended as events and the full state is snapshotted every
    * `snapshotEvery` events, so loading stays fast.
    */
  def journaled[F[_]](journals: JournalBuilder[F], typeName: String, snapshotEvery: Int = 100)(implicit A: Async[F]): F[CoordinatorStore[F]] =
    (journals.build(), Ref.of[F, Int](0)).mapN { (journal, sinceSnapshot) =>
      val shard = Shard("xplat-sharding-coordinator", typeName)
      new CoordinatorStore[F] {

        def load: F[Data] =
          for {
            _        <- journal.create(shard)
            snapshot <- journal.getSnapshot(shard)
            initial  <- A.delay(snapshot.flatMap(_.data).fold(Data.empty)(decodeData))
            events   <- journal.getEvents(shard)
            data     <- events.compile.fold(initial)((d, e) => applyEvent(d, decodeEvent(e.data)))
          } yield data

        def save(before: Data, after: Data): F[Unit] = {
          val changes = diff(before, after)
          A.whenA(changes.nonEmpty) {
            journal.writeEvents(shard, changes.map(encodeEvent)) >>
              sinceSnapshot.modify { n =>
                val total = n + changes.size
                if (total >= snapshotEvery) (0, true) else (total, false)
              }.ifM(journal.writeSnapshot(shard, encodeData(after)), A.unit)
          }
        }
      }
    }

  // ---- Events ----

  sealed trait Event
  final case class Allocated(shard: String, region: Address) extends Event
  final case class Deallocated(shard: String) extends Event
  final case class Remembered(shard: String, entities: Set[String]) extends Event

  def diff(before: Data, after: Data): Vector[Event] = {
    val allocated = after.allocations.toVector.sortBy(_._1).collect { case (s, r) if !before.allocations.get(s).contains(r) => Allocated(s, r) }
    val deallocated = (before.allocations.keySet -- after.allocations.keySet).toVector.sorted.map(Deallocated)
    val shards = (before.remembered.keySet ++ after.remembered.keySet).toVector.sorted
    val remembered = shards.collect {
      case s if before.remembered.getOrElse(s, Set.empty) != after.remembered.getOrElse(s, Set.empty) => Remembered(s, after.remembered.getOrElse(s, Set.empty))
    }
    allocated ++ deallocated ++ remembered
  }

  def applyEvent(data: Data, event: Event): Data = event match {
    case Allocated(s, r)  => data.copy(allocations = data.allocations.updated(s, r))
    case Deallocated(s)   => data.copy(allocations = data.allocations - s)
    case Remembered(s, e) => data.copy(remembered = if (e.isEmpty) data.remembered - s else data.remembered.updated(s, e))
  }

  private def address(w: BinWriter, a: Address): Unit = { w.string(a.system).string(a.host).int(a.port); () }
  private def address(r: BinReader): Address = Address(r.string(), r.string(), r.int())

  def encodeEvent(e: Event): Array[Byte] = {
    val w = BinWriter()
    e match {
      case Allocated(s, r)  => w.byte(1).string(s); address(w, r)
      case Deallocated(s)   => w.byte(2).string(s)
      case Remembered(s, e) => w.byte(3).string(s).seq(e)(_.string(_))
    }
    w.result()
  }

  def decodeEvent(bytes: Array[Byte]): Event = {
    val r = BinReader(bytes)
    r.byte() match {
      case 1 => Allocated(r.string(), address(r))
      case 2 => Deallocated(r.string())
      case 3 => Remembered(r.string(), r.seq(_.string()).toSet)
      case n => throw new IllegalStateException(s"unknown coordinator event $n")
    }
  }

  def encodeData(d: Data): Array[Byte] =
    BinWriter()
      .seq(d.allocations) { case (w, (s, r)) => w.string(s); address(w, r) }
      .seq(d.remembered) { case (w, (s, e)) => w.string(s).seq(e)(_.string(_)) }
      .result()

  def decodeData(bytes: Array[Byte]): Data = {
    val r = BinReader(bytes)
    Data(r.seq(r => r.string() -> address(r)).toMap, r.seq(r => r.string() -> r.seq(_.string()).toSet).toMap)
  }
}
