package xplat.core.io.actor

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api._
import xplat.core.api.error.CodecError
import xplat.core.io.actor.ActorTestKit._
import xplat.core.persistence.Persistence
import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}
import xplat.core.persistence.api.{Effect, Effects, Journal, JournalBuilder}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class IOPersistenceTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import IOPersistenceTest._

  private val shard = Shard("acc", "acc")

  "IOPersistence" - {

    "a new entity starts from the initial state" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        ref     <- spawnAccumulator(system, journal)
        value   <- ref.ask[Int](Current(_))
      } yield value shouldBe 0
    }

    "persisted events update the state and are written to the journal" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        ref     <- spawnAccumulator(system, journal)
        _       <- ref ! Add(2)
        _       <- ref ! Add(3)
        value   <- ref.ask[Int](Current(_))
        events  <- journal.storedEvents(shard)
      } yield (value, events) shouldBe (5, Vector("2", "3"))
    }

    "the state is recovered by replaying the journal events" in withSystem { system =>
      for {
        journal <- InMemoryJournal(events = Vector("1", "2", "3"))
        ref     <- spawnAccumulator(system, journal)
        value   <- ref.ask[Int](Current(_))
      } yield value shouldBe 6
    }

    "the state is recovered from the snapshot plus the events after it" in withSystem { system =>
      for {
        journal <- InMemoryJournal(snapshot = Some("10"), events = Vector("5"))
        ref     <- spawnAccumulator(system, journal)
        value   <- ref.ask[Int](Current(_))
      } yield value shouldBe 15
    }

    "compress writes a snapshot of the current state" in withSystem { system =>
      for {
        journal  <- InMemoryJournal()
        ref      <- spawnAccumulator(system, journal)
        _        <- ref ! Add(7)
        _        <- ref ! Snapshot
        _        <- ref.ask[Int](Current(_))
        snapshot <- journal.storedSnapshot(shard)
      } yield snapshot shouldBe Some("7")
    }

    "a restarted entity recovers the state written by the previous one" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        first   <- spawnAccumulator(system, journal)
        _       <- first ! Add(4)
        _       <- first ! Snapshot
        _       <- first ! Add(1)
        _       <- first ! Shutdown
        _       <- awaitStopped(first ! Add(100))
        // The name is released asynchronously, so retry until the new actor can take it.
        second <- spawnAccumulator(system, journal).handleErrorWith(_ => IO.sleep(10.millis) >> spawnAccumulator(system, journal))
        value  <- second.ask[Int](Current(_))
      } yield value shouldBe 5
    }

    "ignore stops processing the remaining effects" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        ref     <- spawnAccumulator(system, journal)
        _       <- ref ! AddIgnored(9)
        value   <- ref.ask[Int](Current(_))
        events  <- journal.storedEvents(shard)
      } yield (value, events) shouldBe (0, Vector.empty)
    }

    "an event that cannot be encoded is not written to the journal" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        ref     <- spawnAccumulator(system, journal)
        _       <- ref ! Add(-1)
        _       <- awaitStopped(ref ! Add(1))
        events  <- journal.storedEvents(shard)
      } yield events shouldBe Vector.empty
    }

    "commands stashed while locked are replayed after unlock" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        locked  <- Ref.of[IO, Boolean](false)
        ref     <- spawnAccumulator(system, journal, handler = lockableHandler(locked))
        _       <- ref ! Lock
        _       <- ref ! Add(2)
        _       <- ref ! Add(3)
        before  <- ref.ask[Int](Current(_))
        _       <- ref ! Unlock
        after   <- awaitUntil(ref.ask[Int](Current(_)))(_ == 5)
        events  <- journal.storedEvents(shard)
      } yield (before, after, events) shouldBe (0, 5, Vector("2", "3"))
    }

    "the journal is closed when the entity stops" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        ref     <- spawnAccumulator(system, journal)
        _       <- ref ! Shutdown
        _       <- awaitUntil(journal.isClosed)(identity)
      } yield succeed
    }

    "an idle entity shuts itself down" in withSystem { system =>
      for {
        journal <- InMemoryJournal()
        ref     <- spawnAccumulator(system, journal, config = Persistence.Config(Some(100.millis)))
        _       <- ref.ask[Int](Current(_))
        // Stay quiet: every message restarts the idle timer.
        _      <- IO.sleep(500.millis)
        result <- (ref ! Add(1)).attempt
      } yield result.isLeft shouldBe true
    }
  }

  private def spawnAccumulator(
      system: ActorSystem[IO],
      journal: InMemoryJournal,
      handler: (Int, AccCmd) => IO[Effects[Int]] = commandHandler,
      config: Persistence.Config = Persistence.Config(shutdownOnIdle = None)
  ): IO[ActorRef[IO, AccCmd]] = {
    implicit val journalBuilder: JournalBuilder[IO] = () => IO.pure(journal)
    val persistence: Persistence[IO] = IOPersistence.mkIOPersistence[IO]
    val behavior = persistence.build[Int, AccCmd, Int]("acc", 0, handler, _ + _, config)
    // The journal shard is Shard(persistence name, actor name), i.e. Shard("acc", "acc").
    system.spawn[AccCmd]("acc")(behavior)
  }
}

object IOPersistenceTest {

  sealed trait AccCmd
  final case class Add(n: Int) extends AccCmd
  final case class AddIgnored(n: Int) extends AccCmd
  case object Lock extends AccCmd
  case object Unlock extends AccCmd
  case object Snapshot extends AccCmd
  case object Shutdown extends AccCmd
  final case class Current(replyTo: ReplyTo[Int]) extends AccCmd

  def commandHandler(state: Int, cmd: AccCmd): IO[Effects[Int]] = cmd match {
    case Add(n)        => IO.pure(Effects(Seq(Effect.Persist(n))))
    case AddIgnored(n) => IO.pure(Effects(Seq(Effect.Ignore, Effect.Persist(n))))
    case Lock | Unlock => IO.pure(Effects(Nil))
    case Snapshot      => IO.pure(Effects(Seq(Effect.Compress)))
    case Shutdown      => IO.raiseError(new RuntimeException("shutdown requested"))
    case Current(r)    => IO(r ! state).as(Effects(Nil))
  }

  /** While locked, Add commands are stashed; Unlock replays them. */
  def lockableHandler(locked: Ref[IO, Boolean])(state: Int, cmd: AccCmd): IO[Effects[Int]] = cmd match {
    case Lock   => locked.set(true).as(Effects(Nil))
    case Unlock => locked.set(false).as(Effects(Seq(Effect.Unstash)))
    case Add(n) => locked.get.map(l => if (l) Effects(Seq(Effect.Stash)) else Effects(Seq(Effect.Persist(n))))
    case other  => commandHandler(state, other)
  }

  /** Encodes ints as UTF-8 text. Negative numbers cannot be encoded, to exercise codec failures. */
  implicit val intCodec: Codec[Int, Array[Byte]] = new Codec[Int, Array[Byte]] {
    def encode(from: Int): Either[CodecError, Array[Byte]] =
      if (from < 0) Left(CodecError(s"negative: $from")) else Right(from.toString.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, Int] =
      new String(to, UTF_8).toIntOption.toRight(CodecError("not a number"))
  }

  /** Journal kept in memory. A snapshot hides the events written before it, so recovery only replays newer events. */
  final class InMemoryJournal private (state: Ref[IO, InMemoryJournal.Data], closed: Ref[IO, Boolean]) extends Journal[IO] {
    import InMemoryJournal.Data

    private def entry(shard: Shard)(d: Data): (Vector[Array[Byte]], Int, Option[Array[Byte]]) =
      (d.events.getOrElse(shard, Vector.empty), d.snapshotOffset.getOrElse(shard, 0), d.snapshots.get(shard))

    def create(shard: Shard): IO[Unit] = IO.unit

    def getEvents(shard: Shard): IO[fs2.Stream[IO, JournalEvent]] = state.get.map { d =>
      val (events, offset, _) = entry(shard)(d)
      fs2.Stream.emits(events.zipWithIndex.drop(offset).map { case (data, i) =>
        JournalEvent(shard.name, shard.id, i.toLong, 0L, data)
      })
    }

    def getSnapshot(shard: Shard): IO[Option[JournalSnapshot]] = state.get.map { d =>
      val (_, offset, snapshot) = entry(shard)(d)
      snapshot.map(data => JournalSnapshot(shard.name, shard.id, offset.toLong, offset.toLong, Some(data)))
    }

    def writeSnapshot(shard: Shard, data: Array[Byte]): IO[Unit] = state.update { d =>
      val (events, _, _) = entry(shard)(d)
      d.copy(snapshots = d.snapshots.updated(shard, data), snapshotOffset = d.snapshotOffset.updated(shard, events.size))
    }

    def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): IO[Unit] = state.update { d =>
      val (events, _, _) = entry(shard)(d)
      d.copy(events = d.events.updated(shard, events ++ entries))
    }

    def close(): IO[Unit] = closed.set(true)

    def storedEvents(shard: Shard): IO[Vector[String]] = state.get.map(entry(shard)(_)._1.map(new String(_, UTF_8)))
    def storedSnapshot(shard: Shard): IO[Option[String]] = state.get.map(entry(shard)(_)._3.map(new String(_, UTF_8)))
    def isClosed: IO[Boolean] = closed.get
  }

  object InMemoryJournal {
    final case class Data(
        events: Map[Shard, Vector[Array[Byte]]],
        snapshots: Map[Shard, Array[Byte]],
        snapshotOffset: Map[Shard, Int]
    )

    /** Builds a journal for the "acc" shard, optionally pre-filled with a snapshot and the events after it. */
    def apply(snapshot: Option[String] = None, events: Vector[String] = Vector.empty): IO[InMemoryJournal] = {
      val shard = Shard("acc", "acc")
      val data = Data(
        events = Map(shard -> events.map(_.getBytes(UTF_8))),
        snapshots = snapshot.map(s => shard -> s.getBytes(UTF_8)).toMap,
        snapshotOffset = Map.empty
      )
      (Ref.of[IO, Data](data), Ref.of[IO, Boolean](false)).mapN(new InMemoryJournal(_, _))
    }
  }
}
