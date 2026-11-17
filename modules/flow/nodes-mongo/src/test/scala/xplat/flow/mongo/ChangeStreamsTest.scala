package xplat.flow.mongo

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import com.mongodb.client.MongoClients
import io.circe.Json
import org.bson.Document
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.connectors.{Incoming, Sink}
import xplat.extensions.persistence.testkit.Containers
import xplat.flow.nodes.Connections

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.concurrent.duration._

/** A one-member replica set, which is what change streams need. */
object ChangeStreamsTest {
  lazy val started: Option[(Containers.Container, IO[Unit])] =
    if (!Containers.available) None
    else
      Some(
        Containers
          .container("docker.io/library/mongo:7", 27017, args = Seq("--replSet", "rs0")) { c =>
            IO.blocking {
              val client = MongoClients.create(s"mongodb://${c.host}:${c.port}/?directConnection=true&serverSelectionTimeoutMS=2000")
              try {
                client.getDatabase("admin").runCommand(new Document("replSetInitiate", new Document("_id", "rs0").append("members", java.util.List.of(new Document("_id", 0).append("host", "localhost:27017")))))
                ()
              } catch { case e: com.mongodb.MongoCommandException if e.getErrorCodeName == "AlreadyInitialized" => () }
              finally client.close()
            } >> waitPrimary(c)
          }
          .allocated
          .unsafeRunSync()
      )

  private def waitPrimary(c: Containers.Container): IO[Unit] =
    IO.blocking {
      val client = MongoClients.create(s"mongodb://${c.host}:${c.port}/?directConnection=true")
      try client.getDatabase("admin").runCommand(new Document("hello", 1)).getBoolean("isWritablePrimary", false)
      finally client.close()
    }.flatMap(primary => if (primary) IO.unit else IO.sleep(300.millis) >> waitPrimary(c))

  sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
}

class ChangeStreamsTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def body(m: Incoming[Array[Byte]]): Json = io.circe.parser.parse(new String(m.value, UTF_8)).toOption.get

  private def eventually[A](io: IO[Option[A]], left: Int = 100): IO[A] =
    io.flatMap {
      case Some(a)          => IO.pure(a)
      case None if left > 0 => IO.sleep(100.millis) >> eventually(io, left - 1)
      case None             => IO.raiseError(new AssertionError("timed out"))
    }

  "the changes of a collection arrive as messages, and a consumer that stops goes on from where it was" in {
    ChangeStreamsTest.started match {
      case None => IO[org.scalatest.Assertion](cancel("no container runtime"))
      case Some((c, _)) =>
        val db = "cs" + UUID.randomUUID().toString.take(8)
        val conn = Connections.Resolved("main", MongoKinds.Mongo, Json.obj("uri" -> Json.fromString(s"mongodb://${c.host}:${c.port}/?directConnection=true"), "database" -> Json.fromString(db)), Map.empty)
        val client = MongoClients.create(s"mongodb://${c.host}:${c.port}/?directConnection=true")
        val orders = client.getDatabase(db).getCollection("orders")
        def write(f: => Unit): IO[Unit] = IO.blocking(f)
        for {
          got <- Ref.of[IO, Vector[Incoming[Array[Byte]]]](Vector.empty)
          sink = Sink[IO, Array[Byte]](m => got.update(_ :+ m))
          first <- ChangeStreams.consume[IO](conn, "orders", "g1", sink).use { _ =>
            IO.sleep(500.millis) >>
              write(orders.insertOne(new Document("_id", 1).append("status", "new"))) >>
              write(orders.updateOne(new Document("_id", 1), new Document("$set", new Document("status", "paid")))) >>
              eventually(got.get.map(ms => Option.when(ms.size >= 2)(ms)))
          }
          // While nothing reads, a change happens; the next consumer of the same group gets it.
          _ <- write(orders.deleteOne(new Document("_id", 1)))
          _ <- got.set(Vector.empty)
          second <- ChangeStreams.consume[IO](conn, "orders", "g1", sink).use(_ => eventually(got.get.map(ms => ms.headOption)))
          _ <- IO.blocking(client.close())
        } yield {
          body(first(0)).hcursor.get[String]("operationType") shouldBe Right("insert")
          body(first(0)).hcursor.downField("fullDocument").get[String]("status") shouldBe Right("new")
          first(0).key shouldBe Some("1")
          body(first(1)).hcursor.downField("fullDocument").get[String]("status") shouldBe Right("paid")
          body(first(1)).hcursor.downField("updateDescription").downField("updatedFields").get[String]("status") shouldBe Right("paid")
          first(1).headers("operation-type") shouldBe "update"
          first.map(_.position).distinct.size shouldBe 2
          body(second).hcursor.get[String]("operationType") shouldBe Right("delete")
          body(second).hcursor.downField("fullDocument").focus shouldBe Some(Json.Null)
        }
    }
  }
}
