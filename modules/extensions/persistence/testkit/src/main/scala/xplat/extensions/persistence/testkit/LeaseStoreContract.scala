package xplat.extensions.persistence.testkit

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.coordination.LeaseSettings
import xplat.core.io.coordination.{LeaseStore, StoreLeaseProvider}

import java.util.UUID
import scala.concurrent.duration._

/** Tests every [[LeaseStore]] implementation must pass. Each test uses its own random lease names. */
abstract class LeaseStoreContract extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  def stores: Resource[IO, LeaseStore[IO]]

  def available: Boolean = true

  private def name(): String = "lease-" + UUID.randomUUID().toString.take(8)

  private def withStore[A](f: LeaseStore[IO] => IO[A]): IO[A] = {
    assume(available, "the store is not available")
    stores.use(f)
  }

  "LeaseStore contract" - {

    "one owner at a time, and the owner can take it again" in withStore { store =>
      val l = name()
      for {
        a      <- store.tryAcquire(l, "a", 1.minute)
        b      <- store.tryAcquire(l, "b", 1.minute)
        again  <- store.tryAcquire(l, "a", 1.minute)
        holder <- store.holder(l)
      } yield (a, b, again, holder) shouldBe (true, false, true, Some("a"))
    }

    "an expired lease is free" in withStore { store =>
      val l = name()
      for {
        _      <- store.tryAcquire(l, "a", 300.millis)
        _      <- IO.sleep(1.second)
        holder <- store.holder(l)
        renew  <- store.renew(l, "a", 1.minute)
        b      <- store.tryAcquire(l, "b", 1.minute)
      } yield (holder, renew, b) shouldBe (None, false, true)
    }

    "renewing extends the lease" in withStore { store =>
      val l = name()
      for {
        _      <- store.tryAcquire(l, "a", 1.second)
        _      <- IO.sleep(600.millis)
        ok     <- store.renew(l, "a", 1.second)
        _      <- IO.sleep(600.millis)
        holder <- store.holder(l)
      } yield (ok, holder) shouldBe (true, Some("a"))
    }

    "only the owner renews or releases" in withStore { store =>
      val l = name()
      for {
        _        <- store.tryAcquire(l, "a", 1.minute)
        renewB   <- store.renew(l, "b", 1.minute)
        releaseB <- store.release(l, "b")
        releaseA <- store.release(l, "a")
        holder   <- store.holder(l)
        b        <- store.tryAcquire(l, "b", 1.minute)
      } yield (renewB, releaseB, releaseA, holder, b) shouldBe (false, false, true, None, true)
    }

    "concurrent owners: exactly one wins" in withStore { store =>
      val l = name()
      (1 to 10).toList.parTraverse(i => store.tryAcquire(l, s"owner-$i", 1.minute)).map(_.count(identity) shouldBe 1)
    }

    "works behind StoreLeaseProvider" in withStore { store =>
      val l = name()
      val provider = StoreLeaseProvider[IO](store)
      def settings(owner: String) = LeaseSettings(l, owner, timeToLive = 1.second, heartbeatInterval = 200.millis)
      (provider.lease(settings("a")), provider.lease(settings("b"))).tupled.use { case (a, b) =>
        for {
          gotA <- a.acquire
          _    <- IO.sleep(2.seconds) // past the time to live: only renewals keep it
          gotB <- b.acquire
        } yield (gotA, gotB) shouldBe (true, false)
      }
    }
  }
}
