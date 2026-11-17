package xplat.flow.db

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._
import xplat.flow.nodes.{Connections, NodeType}

import java.nio.file.{Files, Path}

class DbNodesTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, j => j)

  private def tempFile(): Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempFile("flow-db-", ".sqlite")))(p => IO.blocking(Files.deleteIfExists(p)).void)

  private def sqlite(name: String, file: Path): Connections.Resolved = Connections.Resolved(name, DbKinds.Sqlite, Json.obj("file" -> Json.fromString(file.toString)), Map.empty)

  private final class Env(val connections: Map[String, Connections.Resolved], val pool: DbPool[IO]) {
    private val registry: Connections[IO] = (name: String) => IO.pure(connections.get(name))
    val query = new QueryNode[IO](registry, pool)
    val execute = new ExecuteNode[IO](registry, pool)
    val insert = new InsertNode[IO](registry, pool)
    val update = new UpdateNode[IO](registry, pool)
    val delete = new DeleteNode[IO](registry, pool)

    def run(node: NodeType[IO], params: String, state: String = "{}", outputs: List[String] = Nil): IO[NodeOutput] =
      node.run(NodeInput(json(params), json(state), instanceId = "i1", nodeId = "n1", outputs = outputs.map(n => OutputDecl(n, StatePath(List(n)), DataType.AnyJson))))
  }

  private def withEnv[A](f: Env => IO[A]): IO[A] =
    (for {
      file <- tempFile()
      pool <- DbPool.make[IO]
    } yield new Env(Map("main" -> sqlite("main", file)), pool)).use(f)

  private def setup(env: Env): IO[Unit] =
    for {
      _ <- env.run(env.execute, """{"connection": "main", "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, score REAL, note TEXT, active BOOLEAN)"}""")
      _ <- env.run(env.insert, """{"connection": "main", "table": "users", "values": {"id": 1, "name": {"value": "Ana"}, "age": {"value": 30}, "score": {"value": 1.5}, "active": {"value": true}}}""")
      _ <- env.run(env.insert, """{"connection": "main", "table": "users", "values": {"id": 2, "name": {"value": "Luis"}, "age": {"value": 41}, "score": {"value": 2.25}, "note": {"value": "hi"}, "active": {"value": false}}}""")
      _ <- env.run(env.insert, """{"connection": "main", "table": "users", "values": {"id": 3, "name": {"value": "Eva"}, "age": {"value": 25}}}""")
    } yield ()

  "query" - {

    "reads rows as objects, with parameters taken from the state" in withEnv { env =>
      for {
        _ <- setup(env)
        out <- env.run(env.query, """{"connection": "main", "sql": "SELECT id, name, age FROM users WHERE age > ? ORDER BY id", "params": [{"path": "limit"}]}""", """{"limit": 26}""")
      } yield {
        out.failed shouldBe false
        out.values("rows") shouldBe json("""[{"id": 1, "name": "Ana", "age": 30}, {"id": 2, "name": "Luis", "age": 41}]""")
        out.values("count") shouldBe Json.fromInt(2)
        out.info("rows") shouldBe Json.fromInt(2)
        out.info("truncated") shouldBe Json.False
        out.info("statement").asString.get should include("SELECT id")
      }
    }

    "columns become JSON by their type: numbers, text, null and booleans" in withEnv { env =>
      for {
        _ <- setup(env)
        out <- env.run(env.query, """{"connection": "main", "sql": "SELECT id, name, score, note, active FROM users ORDER BY id"}""")
      } yield out.values("rows") shouldBe json(
        """[{"id": 1, "name": "Ana", "score": 1.5, "note": null, "active": true},
          | {"id": 2, "name": "Luis", "score": 2.25, "note": "hi", "active": false},
          | {"id": 3, "name": "Eva", "score": null, "note": null, "active": null}]""".stripMargin
      )
    }

    "binary comes as base64" in withEnv { env =>
      for {
        out <- env.run(env.query, """{"connection": "main", "sql": "SELECT x'48656c6c6f' AS data"}""")
      } yield out.values("rows") shouldBe json("""[{"data": "SGVsbG8="}]""")
    }

    "keeps at most maxRows and says there were more" in withEnv { env =>
      for {
        _ <- setup(env)
        out <- env.run(env.query, """{"connection": "main", "sql": "SELECT id FROM users ORDER BY id", "maxRows": 2}""")
      } yield {
        out.values("rows") shouldBe json("""[{"id": 1}, {"id": 2}]""")
        out.info("truncated") shouldBe Json.True
      }
    }

    "answers only the values that the node declared, which is what the engine accepts" in withEnv { env =>
      for {
        _ <- setup(env)
        first <- env.run(env.query, """{"connection": "main", "sql": "SELECT id FROM users ORDER BY id"}""", outputs = List("first"))
        both <- env.run(env.query, """{"connection": "main", "sql": "SELECT id FROM users ORDER BY id"}""", outputs = List("rows", "count"))
        unknown <- env.run(env.query, """{"connection": "main", "sql": "SELECT id FROM users"}""", outputs = List("rowz"))
        nothing <- env.run(env.query, """{"connection": "main", "sql": "SELECT id FROM users WHERE id = 99"}""", outputs = List("first"))
      } yield {
        first.values shouldBe Map("first" -> json("""{"id": 1}"""))
        both.values.keySet shouldBe Set("rows", "count")
        unknown.failed shouldBe true
        unknown.error.get should include("rowz")
        nothing.values shouldBe Map("first" -> Json.Null)
      }
    }

    "a value that is text is data, never part of the statement" in withEnv { env =>
      val evil = "x'; DROP TABLE users; --"
      for {
        _ <- setup(env)
        _ <- env.run(env.insert, """{"connection": "main", "table": "users", "values": {"id": 4, "name": {"path": "name"}}}""", Json.obj("name" -> Json.fromString(evil)).noSpaces)
        found <- env.run(env.query, """{"connection": "main", "sql": "SELECT name FROM users WHERE name = ?", "params": [{"path": "name"}]}""", Json.obj("name" -> Json.fromString(evil)).noSpaces)
        all <- env.run(env.query, """{"connection": "main", "sql": "SELECT count(*) AS n FROM users"}""")
      } yield {
        found.values("rows") shouldBe Json.arr(Json.obj("name" -> Json.fromString(evil)))
        all.values("rows") shouldBe json("""[{"n": 4}]""")
      }
    }

    "it cannot write: a statement that is not a read is refused when saved, and one that writes anyway is stopped by the connection" in withEnv { env =>
      for {
        _ <- setup(env)
        sneaky <- env.run(env.query, """{"connection": "main", "sql": "WITH gone AS (SELECT 1) DELETE FROM users"}""")
        after <- env.run(env.query, """{"connection": "main", "sql": "SELECT count(*) AS n FROM users"}""")
        // the connection is writable again for the nodes that write
        written <- env.run(env.execute, """{"connection": "main", "sql": "DELETE FROM users WHERE id = 3"}""")
      } yield {
        QueryNode.descriptor.checkParams(json("""{"connection": "main", "sql": "DELETE FROM users"}""")).head should include("only reads")
        QueryNode.descriptor.checkParams(json("""{"connection": "main", "sql": "INSERT INTO users(id) VALUES (9)"}""")).head should include("only reads")
        sneaky.failed shouldBe true
        after.values("rows") shouldBe json("""[{"n": 3}]""")
        written.values("affected") shouldBe Json.fromInt(1)
      }
    }

    "a bad statement is an error of the node, with the message of the database" in withEnv { env =>
      env.run(env.query, """{"connection": "main", "sql": "SELECT * FROM nothing_here"}""").map { out =>
        out.port shouldBe Port.Error
        out.error.get should include("db.query on 'main' failed")
        out.error.get should include("nothing_here")
      }
    }
  }

  "insert, update, delete and execute" - {

    "insert answers how many rows it wrote" in withEnv { env =>
      for {
        _ <- setup(env)
        out <- env.run(env.insert, """{"connection": "main", "table": "users", "values": {"id": 10, "name": {"path": "n"}}}""", """{"n": "Zoe"}""", outputs = List("affected"))
      } yield out.values shouldBe Map("affected" -> Json.fromInt(1))
    }

    "update changes the rows that match, and null is compared with IS NULL" in withEnv { env =>
      for {
        _ <- setup(env)
        one <- env.run(env.update, """{"connection": "main", "table": "users", "set": {"age": {"value": 31}}, "where": {"id": {"value": 1}}}""")
        nulls <- env.run(env.update, """{"connection": "main", "table": "users", "set": {"note": {"value": "none"}}, "where": {"note": null}}""")
        rows <- env.run(env.query, """{"connection": "main", "sql": "SELECT id, age, note FROM users ORDER BY id"}""")
      } yield {
        one.values("affected") shouldBe Json.fromInt(1)
        nulls.values("affected") shouldBe Json.fromInt(2)
        rows.values("rows") shouldBe json("""[{"id": 1, "age": 31, "note": "none"}, {"id": 2, "age": 41, "note": "hi"}, {"id": 3, "age": 25, "note": "none"}]""")
      }
    }

    "delete removes the rows that match" in withEnv { env =>
      for {
        _ <- setup(env)
        out <- env.run(env.delete, """{"connection": "main", "table": "users", "where": {"id": {"path": "id"}}}""", """{"id": 2}""")
        left <- env.run(env.query, """{"connection": "main", "sql": "SELECT id FROM users ORDER BY id"}""")
      } yield {
        out.values("affected") shouldBe Json.fromInt(1)
        left.values("rows") shouldBe json("""[{"id": 1}, {"id": 3}]""")
      }
    }

    "an update or delete with no where refuses to run, unless it is asked for" in withEnv { env =>
      for {
        _ <- setup(env)
        refused <- env.run(env.delete, """{"connection": "main", "table": "users"}""")
        allowed <- env.run(env.delete, """{"connection": "main", "table": "users", "allowAll": true}""")
      } yield {
        refused.failed shouldBe true
        refused.error.get should include("where is empty")
        DeleteNode.descriptor.checkParams(json("""{"connection": "main", "table": "users"}""")).size shouldBe 1
        UpdateNode.descriptor.checkParams(json("""{"connection": "main", "table": "users", "set": {"a": 1}}""")).size shouldBe 1
        allowed.values("affected") shouldBe Json.fromInt(3)
      }
    }

    "names of tables and columns must be plain names" in withEnv { env =>
      for {
        bad <- env.run(env.insert, """{"connection": "main", "table": "users; DROP TABLE users", "values": {"id": 1}}""")
        badColumn <- env.run(env.insert, """{"connection": "main", "table": "users", "values": {"id) VALUES (1); --": 1}}""")
      } yield {
        bad.failed shouldBe true
        badColumn.failed shouldBe true
        InsertNode.descriptor.checkParams(json("""{"connection": "main", "table": "a b", "values": {"x": 1}}""")).head should include("plain table or column name")
      }
    }

    "a write is in one transaction: when the statement fails nothing is left" in withEnv { env =>
      for {
        _ <- setup(env)
        failed <- env.run(env.insert, """{"connection": "main", "table": "users", "values": {"id": 1, "name": {"value": "Dup"}}}""")
        count <- env.run(env.query, """{"connection": "main", "sql": "SELECT count(*) AS n FROM users"}""")
      } yield {
        failed.port shouldBe Port.Error
        count.values("rows") shouldBe json("""[{"n": 3}]""")
      }
    }

    "execute can make a write that a retry does not repeat, with the key of the run" in withEnv { env =>
      val sql = "INSERT INTO runs(key, note) VALUES (?, ?) ON CONFLICT(key) DO NOTHING"
      val params = Json.obj("connection" -> Json.fromString("main"), "sql" -> Json.fromString(sql), "params" -> json("""[{"context": "key"}, {"value": "x"}]"""))
      for {
        _ <- env.run(env.execute, """{"connection": "main", "sql": "CREATE TABLE runs (key TEXT PRIMARY KEY, note TEXT)"}""")
        first <- env.run(env.execute, params.noSpaces)
        again <- env.run(env.execute, params.noSpaces)
        rows <- env.run(env.query, """{"connection": "main", "sql": "SELECT key FROM runs"}""")
      } yield {
        first.values("affected") shouldBe Json.fromInt(1)
        again.values("affected") shouldBe Json.fromInt(0)
        rows.values("rows") shouldBe json("""[{"key": "i1:n1"}]""")
      }
    }
  }

  "problems with the input" - {

    "a path that is not in the state, and a connection that does not exist, are errors of the node" in withEnv { env =>
      for {
        noPath <- env.run(env.query, """{"connection": "main", "sql": "SELECT ?", "params": [{"path": "missing"}]}""")
        noConnection <- env.run(env.query, """{"connection": "nowhere", "sql": "SELECT 1"}""")
      } yield {
        noPath.error.get should include("'missing' is not in the state")
        noConnection.error.get should include("'nowhere' does not exist")
      }
    }

    "the statement and its parameters are checked when the flow is saved" in IO {
      def check(sql: String, params: String) = QueryNode.descriptor.checkParams(json(s"""{"connection": "c", "sql": ${Json.fromString(sql).noSpaces}, "params": $params}"""))
      check("SELECT ?, ?", """[{"value": 1}]""").head should include("2 '?' and params has 1")
      check("SELECT 1", """[{"value": 1}]""").head should include("0 '?'")
      check("SELECT 1; SELECT 2", "[]").head should include("one statement only")
      check("SELECT 1;", "[]") shouldBe Nil
      check("SELECT '?' AS q, \"?\" AS r -- ? \n, ?", """[{"value": 1}]""") shouldBe Nil
      check("SELECT a ?? 'x' FROM t WHERE b = ?", """[{"value": 1}]""") shouldBe Nil
      check("SELECT 1", """[{"colour": "red"}]""").head should include("params")
      check("SELECT 1", """[{"context": "other"}]""").head should include("context must be one of")
    }

    "the descriptors say what the nodes read and what they write" in IO {
      val q = QueryNode.descriptor
      q.sideEffects shouldBe SideEffects.Read
      q.reads(json("""{"connection": "c", "sql": "SELECT ?", "params": [{"path": "a.b"}, {"value": 1}]}""")) shouldBe List(StateRead(StatePath(List("a", "b")), DataType.AnyJson))
      q.defaultOutputs(Json.obj()).map(_.name) shouldBe List("rows")
      List(ExecuteNode.descriptor, InsertNode.descriptor, UpdateNode.descriptor, DeleteNode.descriptor).map(_.sideEffects).distinct shouldBe List(SideEffects.Write)
      DbNodes.catalog.all.map(_.kind) shouldBe List("db.delete", "db.execute", "db.insert", "db.query", "db.update")
      DbNodes.connectionKinds.map(_.kind) shouldBe List("postgres", "mariadb", "sqlite")
    }

    "the nodes are valid in a flow, and the validator finds what they read" in IO {
      val flow = FlowDef(
        "f", "F",
        inputs = List(OutputDecl("id", StatePath(List("id")), DataType.Num)),
        nodes = List(
          NodeDef("q", QueryNode.Kind, json("""{"connection": "main", "sql": "SELECT * FROM users WHERE id = ?", "params": [{"path": "id"}]}"""), outputs = List(DbNode.rowsOutput)),
          NodeDef("bad", QueryNode.Kind, json("""{"connection": "main", "sql": "SELECT ?", "params": [{"path": "missing"}]}"""))
        ),
        edges = List(Edge("q", "bad"))
      )
      FlowValidator.validate(flow, DbNodes.catalog).map(_.code) shouldBe List("state-not-declared")
    }
  }

  "test mode" - {

    "a write does not connect: it answers with the statement and the parameters it would have run" in withEnv { env =>
      // the connection does not even exist
      val out = env.insert.simulated(NodeInput(json("""{"connection": "nowhere", "table": "users", "values": {"id": 1, "name": {"path": "n"}}}"""), json("""{"n": "Ana"}"""), outputs = List(OutputDecl("affected", StatePath(List("a")), DataType.Num))))
      IO {
        val o = out.get
        o.failed shouldBe false
        o.values shouldBe Map("affected" -> Json.fromInt(0))
        o.info("statement") shouldBe Json.fromString("INSERT INTO users (id, name) VALUES (?, ?)")
        o.info("params") shouldBe json("""[1, "Ana"]""")
        o.info("simulated") shouldBe Json.True
      }
    }

    "a query answers no rows, and does not touch the database" in withEnv { env =>
      IO {
        val o = env.query.simulated(NodeInput(json("""{"connection": "nowhere", "sql": "SELECT 1"}"""), json("{}"))).get
        o.values("rows") shouldBe Json.arr()
        o.values("count") shouldBe Json.fromInt(0)
        o.info("simulated") shouldBe Json.True
      }
    }

    "a configuration with problems fails in the simulation too" in withEnv { env =>
      IO(env.delete.simulated(NodeInput(json("""{"connection": "c", "table": "t"}"""), json("{}"))).get.failed shouldBe true)
    }
  }

  "pools" - {

    "the pool of a connection is reused, and a connection that was edited gets a new one" in {
      (for {
        a <- tempFile()
        b <- tempFile()
        pool <- DbPool.make[IO]
      } yield (a, b, pool)).use { case (a, b, pool) =>
        for {
          first <- pool.use(sqlite("db", a))(IO.pure)
          again <- pool.use(sqlite("db", a))(IO.pure)
          edited <- pool.use(sqlite("db", b))(IO.pure)
        } yield {
          first.transactor should be theSameInstanceAs again.transactor
          (edited.transactor eq first.transactor) shouldBe false
          first.kind shouldBe DbKinds.Sqlite
        }
      }
    }

    "after an edit the node uses the new database" in {
      (for {
        a <- tempFile()
        b <- tempFile()
        pool <- DbPool.make[IO]
      } yield (a, b, pool)).use { case (a, b, pool) =>
        @volatile var current = sqlite("db", a)
        val registry: Connections[IO] = (_: String) => IO(Some(current))
        val exec = new ExecuteNode[IO](registry, pool)
        val query = new QueryNode[IO](registry, pool)
        def run(n: NodeType[IO], p: String) = n.run(NodeInput(json(p), json("{}")))
        for {
          _ <- run(exec, """{"connection": "db", "sql": "CREATE TABLE t (v INTEGER)"}""")
          _ <- run(exec, """{"connection": "db", "sql": "INSERT INTO t VALUES (1)"}""")
          inA <- run(query, """{"connection": "db", "sql": "SELECT count(*) AS n FROM t"}""")
          _ = current = sqlite("db", b)
          inB <- run(query, """{"connection": "db", "sql": "SELECT count(*) AS n FROM t"}""")
        } yield {
          inA.values("rows") shouldBe json("""[{"n": 1}]""")
          inB.failed shouldBe true // the other file has no table
        }
      }
    }
  }

  "connections" - {

    "the url of a connection never has the password, and the secret is kept apart" in IO {
      val pg = Connections.Resolved("pg", DbKinds.Postgres, json("""{"host": "db.local", "database": "app", "username": "bob", "ssl": true}"""), Map("password" -> "s3cr3t"))
      val s = DbKinds.jdbc(pg).toOption.get
      s.url shouldBe "jdbc:postgresql://db.local:5432/app?sslmode=require"
      s.url should not include "s3cr3t"
      s.password shouldBe Some("s3cr3t")
      s.user shouldBe Some("bob")
      val maria = DbKinds.jdbc(Connections.Resolved("m", DbKinds.MariaDb, json("""{"host": "h", "port": 3307, "database": "d", "maxPoolSize": 2}"""), Map.empty)).toOption.get
      maria.url shouldBe "jdbc:mariadb://h:3307/d"
      maria.maxPoolSize shouldBe 2
      DbKinds.jdbc(sqlite("s", Path.of("/tmp/x.db"))).toOption.get.maxPoolSize shouldBe 1
      DbKinds.jdbc(Connections.Resolved("x", "kafka", Json.obj(), Map.empty)).isLeft shouldBe true
      DbKinds.jdbc(Connections.Resolved("x", DbKinds.Postgres, Json.obj(), Map.empty)).left.toOption.get should include("no host")
    }

    "the password is not in the error of a connection that fails" in {
      val pg = Connections.Resolved("pg", DbKinds.Postgres, json("""{"host": "127.0.0.1", "port": 1, "database": "app", "username": "bob"}"""), Map("password" -> "s3cr3tPass"))
      val registry: Connections[IO] = (_: String) => IO.pure(Some(pg))
      DbPool.make[IO].use { pool =>
        new QueryNode[IO](registry, pool).run(NodeInput(json("""{"connection": "pg", "sql": "SELECT 1", "timeoutSeconds": 3}"""), json("{}"))).map { out =>
          out.port shouldBe Port.Error
          out.error.get should not include "s3cr3tPass"
          out.error.get should include("db.query on 'pg' failed")
        }
      }
    }
  }
}
