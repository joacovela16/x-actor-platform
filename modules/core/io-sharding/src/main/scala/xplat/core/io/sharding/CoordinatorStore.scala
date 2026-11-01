package xplat.core.io.sharding

import cats.effect.kernel.{Async, Ref}
import cats.effect.syntax.all._
import cats.implicits._
import xplat.core.cluster.api.{Address, MemberStatus}
import xplat.core.io.cluster.IOCluster
import xplat.core.io.net.{BinReader, BinWriter}
import xplat.core.persistence.api.journal.Shard
import xplat.core.persistence.api.{Journal, JournalBuilder}

import scala.concurrent.duration.FiniteDuration

/** Where the coordinator keeps its state, so it survives the coordinator moving or the whole cluster restarting. */
private[sharding] trait CoordinatorStore[F[_]] {
  def load: F[CoordinatorStore.Data]
  def save(before: CoordinatorStore.Data, after: CoordinatorStore.Data): F[Unit]

  /** Called on every coordinator tick with the current state, for stores that need to catch up (e.g. new members). */
  def tick(current: CoordinatorStore.Data): F[Unit]
}

private[sharding] object CoordinatorStore {

  /** What is worth keeping: where each shard lives and which entities to restart. */
  final case class Data(allocations: Map[String, Address], remembered: Map[String, Set[String]])

  object Data {
    val empty: Data = Data(Map.empty, Map.empty)
  }

  /** Keeps a copy of the state on every member that runs sharding, so a coordinator that moves to another node takes
    * over the allocations and remembered entities. Each change gets a higher version; a new coordinator asks every
    * member for its copy and keeps the newest. Members that join later get the state on the next tick.
    *
    * It does not survive the whole cluster stopping; use [[journaled]] for that.
    */
  def replicated[F[_]](cluster: IOCluster[F], replicas: Replicas[F], typeName: String, askTimeout: FiniteDuration)(implicit A: Async[F]): F[CoordinatorStore[F]] =
    (Ref.of[F, Long](0L), Ref.of[F, Set[Address]](Set.empty)).mapN { (version, informed) =>
      new CoordinatorStore[F] {

        private def peers: F[Set[Address]] =
          cluster.state.map(_.members.collect { case m if m.status != MemberStatus.Down && m.status != MemberStatus.Removed => m.address })

        private def push(to: Set[Address], v: Long, data: Data): F[Unit] =
          to.toList.traverse_(a => cluster.remoting.send(a, Replicas.endpoint, Replicas.encodePut(typeName, v, data))) >> informed.update(_ ++ to)

        def load: F[Data] =
          for {
            members <- peers
            copies <- members.toList.parTraverseN(16)(a =>
              cluster.remoting.request(a, Replicas.endpoint, Replicas.encodeGet(typeName), askTimeout).map(Replicas.decodeCopy).attempt.map(_.toOption.flatten)
            )
            local <- replicas.get(typeName)
            newest = (local.toList ++ copies.flatten).maxByOption(_._1)
            _ <- version.set(newest.fold(0L)(_._1))
          } yield newest.fold(Data.empty)(_._2)

        def save(before: Data, after: Data): F[Unit] =
          A.whenA(before != after) {
            for {
              v       <- version.updateAndGet(_ + 1)
              _       <- replicas.put(typeName, v, after)
              members <- peers
              _       <- informed.set(Set.empty)
              _       <- push(members - cluster.self.address, v, after)
            } yield ()
          }

        def tick(current: Data): F[Unit] =
          for {
            members <- peers
            known   <- informed.get
            missing = members - cluster.self.address -- known
            v <- version.get
            _ <- A.whenA(missing.nonEmpty && v > 0)(push(missing, v, current))
          } yield ()
      }
    }

  /** Copies of the coordinator state of every entity type, kept on this node. */
  final class Replicas[F[_]] private (copies: Ref[F, Map[String, (Long, Data)]])(implicit A: Async[F]) {

    def get(typeName: String): F[Option[(Long, Data)]] = copies.get.map(_.get(typeName))

    /** Keeps the copy if it is newer than the one held. */
    def put(typeName: String, version: Long, data: Data): F[Unit] =
      copies.update(c => if (c.get(typeName).exists(_._1 >= version)) c else c.updated(typeName, (version, data)))
  }

  object Replicas {
    val endpoint: String = "/system/sharding/coordinator-state"

    /** Creates the replicas of this node and serves them to the coordinators. */
    def apply[F[_]](cluster: IOCluster[F])(implicit A: Async[F]): F[Replicas[F]] =
      Ref.of[F, Map[String, (Long, Data)]](Map.empty).map(new Replicas[F](_)).flatTap { replicas =>
        cluster.remoting.on(endpoint) { bytes =>
          A.delay(decodePut(bytes)).flatMap { case (typeName, v, data) => replicas.put(typeName, v, data) }
        } >> cluster.remoting.onRequest(endpoint) { bytes =>
          A.delay(BinReader(bytes).string()).flatMap(replicas.get).map(encodeCopy)
        }
      }

    def encodeGet(typeName: String): Array[Byte] = BinWriter().string(typeName).result()

    def encodePut(typeName: String, version: Long, data: Data): Array[Byte] =
      BinWriter().string(typeName).long(version).bytes(encodeData(data)).result()

    def decodePut(bytes: Array[Byte]): (String, Long, Data) = {
      val r = BinReader(bytes)
      (r.string(), r.long(), decodeData(r.bytes()))
    }

    def encodeCopy(copy: Option[(Long, Data)]): Array[Byte] =
      BinWriter().option(copy) { case (w, (v, d)) => w.long(v).bytes(encodeData(d)); () }.result()

    def decodeCopy(bytes: Array[Byte]): Option[(Long, Data)] = {
      val r = BinReader(bytes)
      r.option(r => (r.long(), decodeData(r.bytes())))
    }
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

        def tick(current: Data): F[Unit] = A.unit
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
