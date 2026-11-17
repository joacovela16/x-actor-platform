package xplat.flow.engine

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import io.circe.Json
import xplat.flow.nodes.{ClientPool, Connections}

import scala.concurrent.duration._

class ClientPoolTest extends EngineSpec {

  /** A kind whose clients are numbers, and that counts what it opens and closes. */
  final class Counting(opened: Ref[IO, Int], closed: Ref[IO, Int], openTime: FiniteDuration = Duration.Zero, failWith: Option[String] = None)
      extends ClientPool.Kind[IO, Int] {
    val id = "test"
    def open(c: Connections.Resolved, extra: String): Resource[IO, Int] =
      Resource.make(IO.sleep(openTime) >> failWith.fold(opened.updateAndGet(_ + 1))(e => IO.raiseError(new RuntimeException(e))))(_ => closed.update(_ + 1))
  }

  private def conn(name: String, host: String = "a") = Connections.Resolved(name, "x", Json.obj("host" -> Json.fromString(host)), Map("password" -> "p"))

  private def counters = (Ref.of[IO, Int](0), Ref.of[IO, Int](0)).tupled

  "the same connection shares one client, opened once even when many ask at the same time" in {
    for {
      pair <- counters
      (opened, closed) = pair
      kind = new Counting(opened, closed, openTime = 50.millis)
      got <- ClientPool.make[IO]().use(pool => List.fill(20)(pool.use(kind, conn("db"))(IO.pure(_))).parSequence)
      o <- opened.get
      c <- closed.get
    } yield {
      got.distinct shouldBe List(1)
      o shouldBe 1
      c shouldBe 1 // closed when the pool is released
    }
  }

  "another configuration (an edit, another environment variable) or another extra is another client" in {
    for {
      pair <- counters
      (opened, closed) = pair
      kind = new Counting(opened, closed)
      n <- ClientPool.make[IO]().use { pool =>
        pool.use(kind, conn("db"))(IO.pure(_)) >> pool.use(kind, conn("db", host = "b"))(IO.pure(_)) >> pool.use(kind, conn("db"), "orders")(IO.pure(_)) >>
          pool.clients.map(_.size)
      }
      o <- opened.get
    } yield {
      n shouldBe 3
      o shouldBe 3
    }
  }

  "a client that fails to open is not kept: the next use tries again" in {
    for {
      pair <- counters
      (opened, closed) = pair
      failing = new Counting(opened, closed, failWith = Some("down"))
      working = new Counting(opened, closed) // the same id, so the same key
      result <- ClientPool.make[IO]().use { pool =>
        for {
          first <- pool.use(failing, conn("db"))(IO.pure(_)).attempt
          empty <- pool.clients
          second <- pool.use(working, conn("db"))(IO.pure(_))
        } yield (first.left.map(_.getMessage), empty, second)
      }
    } yield {
      result._1 shouldBe Left("down")
      result._2 shouldBe Nil
      result._3 shouldBe 1
    }
  }

  "retiring a connection closes its idle clients now and the lent ones when they are returned" in {
    for {
      pair <- counters
      (opened, closed) = pair
      kind = new Counting(opened, closed)
      steps <- ClientPool.make[IO]().use { pool =>
        for {
          _ <- pool.use(kind, conn("idle"))(IO.pure(_))
          gate <- IO.deferred[Unit]
          lent <- pool.use(kind, conn("busy"))(_ => gate.get).start
          _ <- IO.sleep(50.millis)
          _ <- pool.retire("idle") >> pool.retire("busy")
          afterRetire <- closed.get
          listed <- pool.clients
          again <- pool.use(kind, conn("busy"))(IO.pure(_)) // a new one, the retired one is still lent
          _ <- gate.complete(()) >> lent.join
          afterReturn <- closed.get
        } yield (afterRetire, listed.map(c => (c.connection, c.retired, c.inUse)), again, afterReturn)
      }
    } yield {
      steps._1 shouldBe 1
      steps._2 shouldBe List(("busy", true, 1))
      steps._3 shouldBe 3
      steps._4 shouldBe 2
    }
  }

  "idle clients are closed after a while, and lent ones never" in {
    for {
      pair <- counters
      (opened, closed) = pair
      kind = new Counting(opened, closed)
      result <- ClientPool.make[IO](ClientPool.Config(idle = 100.millis)).use { pool =>
        for {
          gate <- IO.deferred[Unit]
          lent <- pool.use(kind, conn("busy"))(_ => gate.get).start
          _ <- pool.use(kind, conn("idle"))(IO.pure(_))
          _ <- IO.sleep(400.millis)
          names <- pool.clients.map(_.map(_.connection))
          _ <- gate.complete(()) >> lent.join
        } yield names
      }
      c <- closed.get
    } yield {
      result shouldBe List("busy")
      c shouldBe 2
    }
  }

  "over the maximum the least recently used idle client is closed; when all are lent, a new one waits and then fails" in {
    for {
      pair <- counters
      (opened, closed) = pair
      kind = new Counting(opened, closed)
      result <- ClientPool.make[IO](ClientPool.Config(max = 2, acquireTimeout = 200.millis)).use { pool =>
        for {
          _ <- pool.use(kind, conn("a"))(IO.pure(_))
          _ <- IO.sleep(10.millis)
          _ <- pool.use(kind, conn("b"))(IO.pure(_))
          _ <- pool.use(kind, conn("c"))(IO.pure(_)) // closes a
          afterEvict <- pool.clients.map(_.map(_.connection))
          gate <- IO.deferred[Unit]
          lent <- (pool.use(kind, conn("b"))(_ => gate.get), pool.use(kind, conn("c"))(_ => gate.get)).parTupled.start
          _ <- IO.sleep(50.millis)
          full <- pool.use(kind, conn("d"))(IO.pure(_)).attempt
          _ <- gate.complete(()) >> lent.join
        } yield (afterEvict, full.left.map(_.isInstanceOf[ClientPool.PoolFull]))
      }
    } yield {
      result._1 shouldBe List("b", "c")
      result._2 shouldBe Left(true)
    }
  }

  "on release it waits for running operations before closing" in {
    for {
      pair <- counters
      (opened, closed) = pair
      kind = new Counting(opened, closed)
      finished <- Ref.of[IO, Boolean](false)
      _ <- ClientPool.make[IO]().use { pool =>
        pool.use(kind, conn("db"))(_ => IO.sleep(150.millis) >> finished.set(true)).start.void >> IO.sleep(30.millis)
      }
      done <- finished.get
      c <- closed.get
    } yield {
      done shouldBe true
      c shouldBe 1
    }
  }
}
