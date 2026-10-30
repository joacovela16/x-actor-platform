package xplat.extensions.persistence.testkit

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api.error.CodecError
import xplat.core.api.{ActorSystem, Codec, Ref}
import xplat.core.io.actor.{IOActorSystem, IOPersistence}
import xplat.core.persistence.Persistence
import xplat.core.persistence.api.journal.Shard
import xplat.core.persistence.api.{Effect, Effects, JournalBuilder}

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.concurrent.duration._

/** Tests every [[xplat.core.persistence.api.Journal]] implementation must pass.
  *
  * `journals` must give a fresh connection to the same store each time it is used, so the tests can check that data
  * survives a restart. Each test uses its own random shards.
  */
abstract class JournalContract extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  /** A journal builder connected to the store under test. */
  def journals: Resource[IO, JournalBuilder[IO]]

  /** Set to false to cancel the tests, e.g. when the database cannot be started. */
  def available: Boolean = true

  private def shard(): Shard = Shard("test-" + UUID.randomUUID().toString.take(8), "id-" + UUID.randomUUID().toString.take(8))
  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
  private def text(b: Array[Byte]): String = new String(b, UTF_8)

  private def withJournal[A](f: xplat.core.persistence.api.Journal[IO] => IO[A]): IO[A] = {
    assume(available, "the store is not available")
    journals.use(b => b.build().flatMap(j => f(j).guarantee(j.close())))
  }

  private def events(j: xplat.core.persistence.api.Journal[IO], s: Shard): IO[List[(Long, String)]] =
    j.getEvents(s).flatMap(_.compile.toList).map(_.map(e => e.offset -> text(e.data)))

  "Journal contract" - {

    "a new shard has no events and no snapshot" in withJournal { j =>
      val s = shard()
      for {
        _        <- j.create(s)
        evs      <- events(j, s)
        snapshot <- j.getSnapshot(s)
      } yield (evs, snapshot) shouldBe (Nil, None)
    }

    "events are read back in order with consecutive offsets" in withJournal { j =>
      val s = shard()
      for {
        _   <- j.create(s)
        _   <- j.writeEvents(s, Seq(bytes("a"), bytes("b")))
        _   <- j.writeEvents(s, Seq(bytes("c")))
        evs <- events(j, s)
      } yield evs shouldBe List(1L -> "a", 2L -> "b", 3L -> "c")
    }

    "events carry their shard, id and a timestamp" in withJournal { j =>
      val s = shard()
      for {
        before <- IO.realTime.map(_.toMillis)
        _      <- j.writeEvents(s, Seq(bytes("a")))
        event  <- j.getEvents(s).flatMap(_.compile.lastOrError)
      } yield {
        (event.shard, event.id) shouldBe (s.name, s.id)
        event.timestamp should be >= before
      }
    }

    "shards are independent" in withJournal { j =>
      val a = shard()
      val sameName = a.copy(id = a.id + "-other")
      val sameId = a.copy(name = a.name + "-other")
      for {
        _  <- j.writeEvents(a, Seq(bytes("a")))
        _  <- j.writeEvents(sameName, Seq(bytes("b")))
        _  <- j.writeEvents(sameId, Seq(bytes("c")))
        ea <- events(j, a)
        eb <- events(j, sameName)
        ec <- events(j, sameId)
      } yield (ea, eb, ec) shouldBe (List(1L -> "a"), List(1L -> "b"), List(1L -> "c"))
    }

    "after a snapshot only newer events are read" in withJournal { j =>
      val s = shard()
      for {
        _        <- j.writeEvents(s, Seq(bytes("a"), bytes("b")))
        _        <- j.writeSnapshot(s, bytes("state-2"))
        _        <- j.writeEvents(s, Seq(bytes("c")))
        snapshot <- j.getSnapshot(s)
        evs      <- events(j, s)
      } yield {
        snapshot.flatMap(_.data).map(text) shouldBe Some("state-2")
        snapshot.map(_.snapshotOffset) shouldBe Some(2L)
        evs shouldBe List(3L -> "c")
      }
    }

    "a newer snapshot replaces the previous one" in withJournal { j =>
      val s = shard()
      for {
        _        <- j.writeEvents(s, Seq(bytes("a")))
        _        <- j.writeSnapshot(s, bytes("one"))
        _        <- j.writeEvents(s, Seq(bytes("b")))
        _        <- j.writeSnapshot(s, bytes("two"))
        snapshot <- j.getSnapshot(s)
        evs      <- events(j, s)
      } yield (snapshot.flatMap(_.data).map(text), evs) shouldBe (Some("two"), Nil)
    }

    "binary payloads survive unchanged" in withJournal { j =>
      val s = shard()
      val payload = Array.tabulate[Byte](512 * 1024)(i => (i % 251).toByte)
      for {
        _        <- j.writeEvents(s, Seq(payload))
        event    <- j.getEvents(s).flatMap(_.compile.lastOrError)
        _        <- j.writeSnapshot(s, payload)
        snapshot <- j.getSnapshot(s)
      } yield (event.data.toSeq, snapshot.flatMap(_.data).map(_.toSeq)) shouldBe (payload.toSeq, Some(payload.toSeq))
    }

    "concurrent writers never produce duplicate offsets" in withJournal { j =>
      val s = shard()
      for {
        results <- (1 to 10).toList.parTraverse(i => j.writeEvents(s, Seq(bytes(s"w$i"))).attempt)
        evs     <- events(j, s)
      } yield {
        val written = results.count(_.isRight)
        evs.map(_._1) shouldBe (1L to written.toLong).toList
        evs.map(_._2).toSet.size shouldBe written
      }
    }

    "data survives reconnecting" in {
      assume(available, "the store is not available")
      val s = shard()
      for {
        _   <- journals.use(_.build().flatMap(j => j.writeEvents(s, Seq(bytes("kept"))) >> j.writeSnapshot(s, bytes("snap"))))
        out <- journals.use(_.build().flatMap(j => (events(j, s), j.getSnapshot(s)).tupled))
      } yield (out._1, out._2.flatMap(_.data).map(text)) shouldBe (Nil, Some("snap"))
    }

    "a persistent actor recovers its state after a restart" in {
      assume(available, "the store is not available")
      journals.use { implicit builder =>
        val id = "acc-" + UUID.randomUUID().toString.take(8)
        def spawn(system: ActorSystem[IO]) =
          system.spawn[JournalContract.Cmd](id)(b => IOPersistence.mkIOPersistence[IO].build[Int, JournalContract.Cmd, Int]("contract", 0, JournalContract.handler, _ + _, Persistence.Config(None))(JournalContract.intCodec, JournalContract.intCodec)(b))
        for {
          _ <- IOActorSystem[IO]("contract").use { system =>
            spawn(system).flatMap(ref => List(1, 2, 3).traverse_(n => ref ! JournalContract.Add(n)) >> ref.ask[Int](JournalContract.Get(_)))
          }
          recovered <- IOActorSystem[IO]("contract").use(system => spawn(system).flatMap(_.ask[Int](JournalContract.Get(_), 10.seconds)))
        } yield recovered shouldBe 6
      }
    }
  }
}

object JournalContract {
  sealed trait Cmd
  final case class Add(n: Int) extends Cmd
  final case class Get(replyTo: Ref[Int]) extends Cmd

  val handler: (Int, Cmd) => IO[Effects[Int]] = {
    case (_, Add(n))     => IO.pure(Effects(Seq(Effect.Persist(n), Effect.Compress)))
    case (state, Get(r)) => IO(r ! state).as(Effects(Nil))
  }

  val intCodec: Codec[Int, Array[Byte]] = new Codec[Int, Array[Byte]] {
    def encode(from: Int): Either[CodecError, Array[Byte]] = Right(from.toString.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, Int] = new String(to, UTF_8).toIntOption.toRight(CodecError("not a number"))
  }
}
