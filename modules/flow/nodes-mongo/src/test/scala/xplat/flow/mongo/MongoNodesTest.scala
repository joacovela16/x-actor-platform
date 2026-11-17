package xplat.flow.mongo

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.extensions.persistence.testkit.Containers
import xplat.flow.nodes.Template
import xplat.flow.model._
import xplat.flow.nodes.{Connections, NodeType}

import java.util.UUID

/** One MongoDB container for the suite; the tests that need it are skipped when there is no container runtime. */
object MongoNodesTest {
  lazy val started: Option[(Containers.Container, IO[Unit])] =
    if (!Containers.available) None
    else Some(Containers.container("docker.io/library/mongo:7", 27017)(_ => IO.unit).allocated.unsafeRunSync())

  sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
}

class MongoNodesTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)

  private def input(params: String, state: String = "{}", outputs: List[String] = Nil): NodeInput =
    NodeInput(json(params), json(state), instanceId = "i1", nodeId = "n1", outputs = outputs.map(n => OutputDecl(n, StatePath(List(n)), DataType.AnyJson)))

  private final class Env(pool: MongoPool[IO], connection: Connections.Resolved) {
    private val registry: Connections[IO] = (name: String) => IO.pure(Option.when(name == "main")(connection))
    val find = new FindNode[IO](registry, pool)
    val aggregate = new AggregateNode[IO](registry, pool)
    val insert = new InsertNode[IO](registry, pool)
    val update = new UpdateNode[IO](registry, pool)
    val delete = new DeleteNode[IO](registry, pool)
    def run(node: NodeType[IO], params: String, state: String = "{}", outputs: List[String] = Nil): IO[NodeOutput] = node.run(input(params, state, outputs))
  }

  /** A fresh database for each test, so they do not see each other's documents. */
  private def withMongo[A](f: Env => IO[A]): IO[A] =
    MongoNodesTest.started match {
      case None => IO(cancel("no container runtime"))
      case Some((c, _)) =>
        val db = "t" + UUID.randomUUID().toString.take(8)
        val conn = Connections.Resolved("main", MongoKinds.Mongo, Json.obj("host" -> Json.fromString(c.host), "port" -> Json.fromInt(c.port), "database" -> Json.fromString(db)), Map.empty)
        MongoPool.make[IO].map(new Env(_, conn)).use(f)
    }

  private def seed(env: Env): IO[Unit] =
    env
      .run(env.insert, """{"connection": "main", "collection": "users", "documents": [
        |{"_id": 1, "name": "Ana", "age": 30, "active": true},
        |{"_id": 2, "name": "Luis", "age": 41, "active": false},
        |{"_id": 3, "name": "Eva", "age": 25, "active": true}]}""".stripMargin)
      .map(out => assert(!out.failed, out.error))

  "templates" - {
    "replace $path and $context anywhere, and keep Extended JSON as it is" in IO {
      val t = json("""{"a": {"$path": "order.id"}, "b": [{"$context": "key"}], "c": {"$oid": "65f0c0ffee0000000000abcd"}, "d": {"$gt": {"$path": "n"}}}""")
      Template.resolve(t, input("{}", """{"order": {"id": 7}, "n": 3}""")) shouldBe Right(
        json("""{"a": 7, "b": ["i1:n1"], "c": {"$oid": "65f0c0ffee0000000000abcd"}, "d": {"$gt": 3}}""")
      )
      Template.reads(t).map(_.toString) should contain theSameElementsAs List("order.id", "n")
    }

    "a path that is not in the state is an error" in IO {
      Template.resolve(json("""{"a": {"$path": "missing"}}"""), input("{}")) shouldBe a[Left[_, _]]
    }

    "broken markers are refused when the flow is saved" in IO {
      FindNode.descriptor.checkParams(json("""{"connection": "main", "collection": "c", "filter": {"x": {"$context": "nope"}}}""")) should not be empty
      AggregateNode.descriptor.checkParams(json("""{"connection": "main", "collection": "c", "pipeline": [{"$out": "other"}]}""")) should not be empty
      DeleteNode.descriptor.checkParams(json("""{"connection": "main", "collection": "c"}""")) should not be empty
      UpdateNode.descriptor.checkParams(json("""{"connection": "main", "collection": "c", "filter": {"a": 1}, "update": {"name": "x"}}""")) should not be empty
      InsertNode.descriptor.checkParams(json("""{"connection": "main", "collection": "system.users", "document": {}}""")) should not be empty
    }
  }

  "documents written as text (what the editor sends)" - {
    "JSON text is read as the document" in IO {
      val p = AggregateNode.params(Json.obj("connection" -> Json.fromString("main"), "collection" -> Json.fromString("c"), "pipeline" -> Json.fromString("[\n  {\n    \"$match\": {\"parentId\": null}\n  }\n]")))
      p.map(_.pipeline) shouldBe Right(Vector(json("""{"$match": {"parentId": null}}""")))
    }

    "keys need no quotes, as in the MongoDB shell, and markers work the same" in IO {
      val p = FindNode.params(json("""{"connection": "main", "collection": "c", "filter": "{parentId: null, status: 'open', owner: {$path: 'user.id'}, _id: ObjectId('65f0c0ffee0000000000abcd')}"}"""))
      p.map(_.filter) shouldBe Right(json("""{"parentId": null, "status": "open", "owner": {"$path": "user.id"}, "_id": {"$oid": "65f0c0ffee0000000000abcd"}}"""))
      FindNode.descriptor.reads(json("""{"connection": "main", "collection": "c", "filter": "{owner: {$path: 'user.id'}}"}""")).map(_.path.toString) shouldBe List("user.id")
    }

    "text that is not valid says which field and why; empty text is no value" in IO {
      AggregateNode.params(json("""{"connection": "main", "collection": "c", "pipeline": "[{$match: {a: }]"}""")).left.toOption.get should startWith("pipeline is not valid:")
      FindNode.params(json("""{"connection": "main", "collection": "c", "filter": "  "}""")).map(_.filter) shouldBe Right(Json.obj())
    }
  }

  "the password goes as a credential, never in the connection string" in IO {
    val c = Connections.Resolved("m", MongoKinds.Mongo, json("""{"host": "db", "database": "app", "username": "u"}"""), Map("password" -> "s3cret"))
    val settings = MongoKinds.client(c).toOption.get
    settings.database shouldBe "app"
    settings.settings.getCredential.getUserName shouldBe "u"
    settings.settings.getCredential.getSource shouldBe "admin"
    settings.settings.getClusterSettings.getHosts.toString should not include "s3cret"
  }

  "writes are simulated in test mode, without connecting" in IO {
    val node = new InsertNode[IO]((_: String) => IO.pure(None), null)
    val out = node.simulated(input("""{"connection": "main", "collection": "c", "document": {"k": {"$context": "key"}}}""")).get
    out.failed shouldBe false
    out.info("simulated") shouldBe Json.True
    out.info("operation") shouldBe json("""{"insert": [{"k": "i1:n1"}]}""")
  }

  "find" - {
    "filters with values of the state, sorts, projects and limits" in withMongo { env =>
      for {
        _ <- seed(env)
        out <- env.run(
          env.find,
          """{"connection": "main", "collection": "users", "filter": {"active": true, "age": {"$gte": {"$path": "min"}}}, "projection": {"name": 1}, "sort": {"age": -1}, "limit": 1}""",
          """{"min": 20}""",
          List("documents", "count")
        )
      } yield {
        out.error shouldBe None
        out.values("documents") shouldBe json("""[{"_id": 1, "name": "Ana"}]""")
        out.values("count") shouldBe Json.fromInt(1)
        out.info("truncated") shouldBe Json.True
      }
    }

    "an unknown connection fails the node" in withMongo { env =>
      env.run(env.find, """{"connection": "other", "collection": "users"}""").map(_.error.get should include("does not exist"))
    }
  }

  "aggregate groups documents" in withMongo { env =>
    for {
      _ <- seed(env)
      out <- env.run(env.aggregate, """{"connection": "main", "collection": "users", "pipeline": [{"$group": {"_id": "$active", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]}""")
    } yield out.values("documents") shouldBe json("""[{"_id": true, "n": 2}, {"_id": false, "n": 1}]""")
  }

  "insert answers the ids, and an ObjectId comes back as Extended JSON" in withMongo { env =>
    for {
      out <- env.run(env.insert, """{"connection": "main", "collection": "orders", "document": {"total": {"$path": "total"}}}""", """{"total": 9.5}""", List("insertedId", "affected"))
      id = out.values("insertedId")
      back <- env.run(env.find, s"""{"connection": "main", "collection": "orders", "filter": {"_id": ${id.noSpaces}}}""", outputs = List("first"))
    } yield {
      out.values("affected") shouldBe Json.fromInt(1)
      id.hcursor.get[String]("$oid").isRight shouldBe true
      back.values("first").hcursor.get[Double]("total") shouldBe Right(9.5)
    }
  }

  "update and delete" - {
    "update changes one or many, and upsert creates" in withMongo { env =>
      for {
        _ <- seed(env)
        one <- env.run(env.update, """{"connection": "main", "collection": "users", "filter": {"_id": 2}, "update": {"$set": {"active": true}}}""", outputs = List("matched", "affected"))
        many <- env.run(env.update, """{"connection": "main", "collection": "users", "filter": {"active": true}, "update": {"$inc": {"age": 1}}, "many": true}""")
        up <- env.run(env.update, """{"connection": "main", "collection": "users", "filter": {"_id": 9}, "update": {"$set": {"name": "Zoe"}}, "upsert": true}""", outputs = List("upsertedId"))
      } yield {
        one.values shouldBe Map("matched" -> Json.fromInt(1), "affected" -> Json.fromInt(1))
        many.values("affected") shouldBe Json.fromInt(3)
        up.values("upsertedId") shouldBe Json.fromInt(9)
      }
    }

    "delete removes the matching documents" in withMongo { env =>
      for {
        _ <- seed(env)
        out <- env.run(env.delete, """{"connection": "main", "collection": "users", "filter": {"active": true}, "many": true}""")
        left <- env.run(env.find, """{"connection": "main", "collection": "users"}""", outputs = List("count"))
      } yield {
        out.values("affected") shouldBe Json.fromInt(2)
        left.values("count") shouldBe Json.fromInt(1)
      }
    }
  }
}
