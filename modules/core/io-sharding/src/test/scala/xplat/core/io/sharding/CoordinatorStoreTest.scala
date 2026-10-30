package xplat.core.io.sharding

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.cluster.api.Address
import xplat.core.io.sharding.CoordinatorStore._

class CoordinatorStoreTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private val regionA = Address("sys", "h", 1)
  private val regionB = Address("sys", "h", 2)

  private val initial = Data(Map("1" -> regionA, "2" -> regionA, "3" -> regionB), Map("1" -> Set("x"), "2" -> Set("y")))
  private val target = Data(Map("1" -> regionB, "3" -> regionB, "4" -> regionA), Map("1" -> Set("x", "z"), "4" -> Set("w")))

  "replaying the diff turns one state into the other" in {
    diff(initial, target).foldLeft(initial)(applyEvent) shouldBe target
  }

  "no change, no events" in {
    diff(target, target) shouldBe empty
  }

  "events and snapshots survive encoding" in {
    diff(initial, target).foreach(e => decodeEvent(encodeEvent(e)) shouldBe e)
    decodeData(encodeData(target)) shouldBe target
  }

  "a journaled store loads what it saved, also target snapshots" in {
    for {
      journal <- SharedJournal()
      store   <- journaled[IO](journal.builder, "type", snapshotEvery = 2)
      _       <- store.save(Data.empty, initial)
      _       <- store.save(initial, target)
      reload  <- journaled[IO](journal.builder, "type").flatMap(_.load)
    } yield reload shouldBe target
  }
}
