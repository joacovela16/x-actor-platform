package xplat.flow.http

import xplat.flow.nodes.ConnectionKind
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._
import xplat.flow.nodes.CoreNodes

import java.nio.file.{Files, Path}

/** What the stores keep in the data directory survives starting them again. */
class StoresTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def withDir[A](f: Path => IO[A]): IO[A] =
    IO.blocking(Files.createTempDirectory("flow-stores")).flatMap(f)

  private val flow = FlowDef("f", "F", List(NodeDef("a", "core.if", Json.obj("path" -> Json.fromString("x")))), inputs = List(OutputDecl("x", StatePath(List("x")), DataType.Bool)))

  "flows keep every revision" in withDir { dir =>
    val file = dir.resolve("flows.json")
    for {
      first <- JsonFlowStore.make[IO](CoreNodes.catalog, Some(file))
      _ <- first.save(flow)
      _ <- first.save(flow.copy(name = "F2"))
      again <- JsonFlowStore.make[IO](CoreNodes.catalog, Some(file))
      latest <- again.get("f", None)
      old <- again.get("f", Some(1))
      next <- again.save(flow.copy(name = "F3"))
      invalid <- again.save(flow.copy(id = "g", edges = List(Edge("a", "ghost"))))
      list <- again.list
    } yield {
      latest.map(_.name) shouldBe Some("F2")
      old.map(_.name) shouldBe Some("F")
      next.map(_.revision) shouldBe Right(3)
      invalid.isLeft shouldBe true
      list shouldBe List(xplat.flow.engine.FlowSummary("f", "F3", 3))
    }
  }

  "triggers, connections and secrets are read back, and the file of secrets is private" in withDir { dir =>
    val trigger = TriggerDef("t", "f", TriggerDef.Kind.Http, path = Some("/x/"), auth = Some(TriggerDef.Auth("apiKey", "c.token")))
    def open = for {
      triggers <- TriggerRegistry.make[IO](Some(dir.resolve("triggers.json")))
      secrets <- SecretStore.make[IO](Some(dir.resolve("secrets.json")))
      connections <- ConnectionStore.make[IO](ConnectionKind.defaults, secrets, Some(dir.resolve("connections.json")))
    } yield (triggers, secrets, connections)
    for {
      first <- open
      (triggers, _, connections) = first
      _ <- triggers.put(trigger)
      _ <- connections.put("c", "secret", Json.obj(), Map("token" -> "s3"))
      second <- open
      (triggers2, secrets2, connections2) = second
      found <- triggers2.route("post", "x")
      view <- connections2.get("c")
      token <- secrets2.get("c.token")
      private_ <- IO.blocking(scala.util.Try(Files.getPosixFilePermissions(dir.resolve("secrets.json")).size).toOption)
      connectionsFile <- IO.blocking(new String(Files.readAllBytes(dir.resolve("connections.json"))))
    } yield {
      found shouldBe Some(trigger)
      view.map(_.secretsSet) shouldBe Some(List("token"))
      token shouldBe Some("s3")
      private_.forall(_ == 2) shouldBe true
      connectionsFile should not include "s3"
    }
  }
}
