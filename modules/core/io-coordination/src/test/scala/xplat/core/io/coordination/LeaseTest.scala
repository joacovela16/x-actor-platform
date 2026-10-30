package xplat.core.io.coordination

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import cats.implicits._
import com.comcast.ip4s._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.coordination.{Lease, LeaseSettings}

import scala.concurrent.duration._

class LeaseTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def settings(owner: String, ttl: FiniteDuration = 1.second): LeaseSettings =
    LeaseSettings("lease", owner, timeToLive = ttl, heartbeatInterval = ttl / 5, operationTimeout = 200.millis)

  /** Store that can be made to fail or to report the lease as taken by someone else. */
  final class ControlledStore(underlying: LeaseStore[IO], failing: Ref[IO, Boolean], stolen: Ref[IO, Boolean]) extends LeaseStore[IO] {
    private def guard[A](op: IO[A]): IO[A] = failing.get.ifM(IO.raiseError(new java.io.IOException("store down")), op)
    def tryAcquire(name: String, owner: String, ttl: FiniteDuration): IO[Boolean] = guard(underlying.tryAcquire(name, owner, ttl))
    def renew(name: String, owner: String, ttl: FiniteDuration): IO[Boolean] = guard(stolen.get.ifM(IO.pure(false), underlying.renew(name, owner, ttl)))
    def release(name: String, owner: String): IO[Boolean] = guard(underlying.release(name, owner))
    def holder(name: String): IO[Option[String]] = guard(underlying.holder(name))
  }

  private def controlled: IO[(ControlledStore, Ref[IO, Boolean], Ref[IO, Boolean])] =
    for {
      store   <- InMemoryLeaseStore[IO]
      failing <- Ref.of[IO, Boolean](false)
      stolen  <- Ref.of[IO, Boolean](false)
    } yield (new ControlledStore(store, failing, stolen), failing, stolen)

  "InMemoryLeaseStore" - {

    "gives a lease to one owner at a time" in {
      for {
        store  <- InMemoryLeaseStore[IO]
        a      <- store.tryAcquire("l", "a", 1.minute)
        b      <- store.tryAcquire("l", "b", 1.minute)
        again  <- store.tryAcquire("l", "a", 1.minute)
        holder <- store.holder("l")
      } yield (a, b, again, holder) shouldBe (true, false, true, Some("a"))
    }

    "frees an expired lease" in {
      for {
        store <- InMemoryLeaseStore[IO]
        _     <- store.tryAcquire("l", "a", 100.millis)
        _     <- IO.sleep(200.millis)
        renew <- store.renew("l", "a", 100.millis)
        b     <- store.tryAcquire("l", "b", 1.minute)
      } yield (renew, b) shouldBe (false, true)
    }

    "only the owner can release" in {
      for {
        store  <- InMemoryLeaseStore[IO]
        _      <- store.tryAcquire("l", "a", 1.minute)
        byB    <- store.release("l", "b")
        byA    <- store.release("l", "a")
        holder <- store.holder("l")
      } yield (byB, byA, holder) shouldBe (false, true, None)
    }
  }

  "StoreLeaseProvider" - {

    "only one lease of the same name is held at a time" in {
      StoreLeaseProvider.inMemory[IO].flatMap { implicit provider =>
        (Lease[IO](settings("a")), Lease[IO](settings("b"))).tupled.use { case (a, b) =>
          for {
            gotA  <- a.acquire
            gotB  <- b.acquire
            heldA <- a.checkLease
            heldB <- b.checkLease
          } yield (gotA, gotB, heldA, heldB) shouldBe (true, false, true, false)
        }
      }
    }

    "renewals keep the lease beyond its time to live" in {
      StoreLeaseProvider.inMemory[IO].flatMap { implicit provider =>
        (Lease[IO](settings("a", ttl = 300.millis)), Lease[IO](settings("b", ttl = 300.millis))).tupled.use { case (a, b) =>
          for {
            _     <- a.acquire
            _     <- IO.sleep(1.second)
            gotB  <- b.acquire
            heldA <- a.checkLease
          } yield (gotB, heldA) shouldBe (false, true)
        }
      }
    }

    "a released lease can be taken by another owner" in {
      StoreLeaseProvider.inMemory[IO].flatMap { implicit provider =>
        (Lease[IO](settings("a")), Lease[IO](settings("b"))).tupled.use { case (a, b) =>
          for {
            _        <- a.acquire
            released <- a.release
            gotB     <- b.acquire
          } yield (released, gotB) shouldBe (true, true)
        }
      }
    }

    "closing the resource releases the lease" in {
      StoreLeaseProvider.inMemory[IO].flatMap { implicit provider =>
        for {
          _    <- Lease[IO](settings("a")).use(_.acquire)
          gotB <- Lease[IO](settings("b")).use(_.acquire)
        } yield gotB shouldBe true
      }
    }

    "the lease is lost when the store says someone else holds it" in {
      controlled.flatMap { case (store, _, stolen) =>
        StoreLeaseProvider[IO](store).lease(settings("a", ttl = 500.millis)).use { lease =>
          for {
            _    <- lease.acquire
            _    <- stolen.set(true)
            _    <- lease.lost.timeout(2.seconds)
            held <- lease.checkLease
          } yield held shouldBe false
        }
      }
    }

    "the lease is lost when renewals fail for its time to live" in {
      controlled.flatMap { case (store, failing, _) =>
        StoreLeaseProvider[IO](store).lease(settings("a", ttl = 500.millis)).use { lease =>
          for {
            _       <- lease.acquire
            _       <- failing.set(true)
            started <- IO.monotonic
            _       <- lease.lost.timeout(3.seconds)
            elapsed <- IO.monotonic.map(_ - started)
          } yield elapsed should be >= 400.millis
        }
      }
    }

    "a short store outage does not lose the lease" in {
      controlled.flatMap { case (store, failing, _) =>
        StoreLeaseProvider[IO](store).lease(settings("a", ttl = 1.second)).use { lease =>
          for {
            _    <- lease.acquire
            _    <- failing.set(true)
            _    <- IO.sleep(300.millis)
            _    <- failing.set(false)
            _    <- IO.sleep(1.second)
            held <- lease.checkLease
          } yield held shouldBe true
        }
      }
    }
  }

  "LeaseServer" - {

    "shares leases between clients over TCP" in {
      val run = for {
        store   <- cats.effect.Resource.eval(InMemoryLeaseStore[IO])
        address <- LeaseServer.bind[IO](host"127.0.0.1", port"0", store)
        clientA <- RemoteLeaseStore[IO](address)
        clientB <- RemoteLeaseStore[IO](address)
      } yield (clientA, clientB)

      run.use { case (a, b) =>
        for {
          gotA    <- a.tryAcquire("l", "a", 1.minute)
          gotB    <- b.tryAcquire("l", "b", 1.minute)
          holder  <- b.holder("l")
          renewed <- a.renew("l", "a", 1.minute)
          freed   <- a.release("l", "a")
          gotB2   <- b.tryAcquire("l", "b", 1.minute)
        } yield (gotA, gotB, holder, renewed, freed, gotB2) shouldBe (true, false, Some("a"), true, true, true)
      }
    }

    "leases work through a remote store" in {
      val run = for {
        store    <- cats.effect.Resource.eval(InMemoryLeaseStore[IO])
        address  <- LeaseServer.bind[IO](host"127.0.0.1", port"0", store)
        remoteA  <- RemoteLeaseStore[IO](address)
        remoteB  <- RemoteLeaseStore[IO](address)
        leaseA   <- StoreLeaseProvider[IO](remoteA).lease(settings("a", ttl = 300.millis))
        leaseB   <- StoreLeaseProvider[IO](remoteB).lease(settings("b", ttl = 300.millis))
      } yield (leaseA, leaseB)

      run.use { case (a, b) =>
        for {
          gotA <- a.acquire
          _    <- IO.sleep(800.millis) // past the time to live: only renewals keep it
          gotB <- b.acquire
        } yield (gotA, gotB) shouldBe (true, false)
      }
    }

    "operations fail when the server cannot be reached" in {
      RemoteLeaseStore[IO](SocketAddress(host"127.0.0.1", port"1")).use { store =>
        store.tryAcquire("l", "a", 1.minute).attempt.map(_.isLeft shouldBe true)
      }
    }
  }
}
