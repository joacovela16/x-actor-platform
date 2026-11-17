package xplat.flow.http

import cats.effect.IO
import io.circe.Json
import io.circe.syntax._
import org.http4s.Status
import xplat.flow.model._

import java.nio.file.Files

class DraftsTest extends HttpSpec {

  /** The approval flow with an edge to a node that does not exist: it does not validate. */
  private val broken: FlowDef = approval.copy(edges = approval.edges :+ Edge("check", "ghost", "then"))

  "a flow that does not validate is kept as a draft, with its errors, and does not run" in withApp { app =>
    for {
      put <- app.put("/api/flows/approval/draft", broken.asJson)
      list <- app.get("/api/flows")
      published <- app.get("/api/flows/approval")
      again <- app.get("/api/flows/approval/draft")
      publish <- app.post("/api/flows/approval/publish", Json.obj())
    } yield {
      put.status shouldBe Status.Ok
      put.json.hcursor.downField("errors").as[List[Json]].toOption.get should not be empty
      put.json.hcursor.downField("flow").get[String]("id") shouldBe Right("approval")
      list.json shouldBe json("""[{"id": "approval", "name": "Approval", "revision": 0, "enabled": false, "draft": true}]""")
      published.status shouldBe Status.NotFound
      again.json.hcursor.downField("flow").downField("edges").as[List[Json]].map(_.size) shouldBe Right(3)
      publish.status shouldBe Status.UnprocessableContent
    }
  }

  "once it validates, publishing makes it a revision and removes the draft" in withApp { app =>
    for {
      _ <- app.put("/api/flows/approval/draft", broken.asJson)
      fixed <- app.put("/api/flows/approval/draft", approval.asJson)
      publish <- app.post("/api/flows/approval/publish", Json.obj())
      draft <- app.get("/api/flows/approval/draft")
      list <- app.get("/api/flows")
    } yield {
      fixed.json.hcursor.downField("errors").as[List[Json]] shouldBe Right(Nil)
      publish.status shouldBe Status.Ok
      publish.json.hcursor.get[Int]("revision") shouldBe Right(1)
      draft.status shouldBe Status.NotFound
      list.json.hcursor.downN(0).get[Boolean]("draft") shouldBe Right(false)
      list.json.hcursor.downN(0).get[Int]("revision") shouldBe Right(1)
    }
  }

  "a draft over a published flow leaves the published one running; saving a revision removes the draft" in withApp { app =>
    for {
      _ <- app.save(approval)
      _ <- app.put("/api/flows/approval/draft", broken.asJson)
      listed <- app.get("/api/flows")
      published <- app.get("/api/flows/approval")
      _ <- app.save(approval.copy(name = "Approval 2"))
      draft <- app.get("/api/flows/approval/draft")
    } yield {
      listed.json.hcursor.downN(0).get[Boolean]("draft") shouldBe Right(true)
      published.json.hcursor.downField("edges").as[List[Json]].map(_.size) shouldBe Right(2)
      draft.status shouldBe Status.NotFound
    }
  }

  "a flow that only has a draft can be deleted, and the draft can be discarded" in withApp { app =>
    for {
      _ <- app.put("/api/flows/approval/draft", broken.asJson)
      discarded <- app.delete("/api/flows/approval/draft")
      gone <- app.get("/api/flows/approval/draft")
      _ <- app.put("/api/flows/approval/draft", broken.asJson)
      deleted <- app.delete("/api/flows/approval")
      list <- app.get("/api/flows")
    } yield {
      discarded.status shouldBe Status.NoContent
      gone.status shouldBe Status.NotFound
      deleted.status shouldBe Status.NoContent
      list.json shouldBe Json.arr()
    }
  }

  "drafts are kept in their slot" in {
    val file = Files.createTempDirectory("drafts-").resolve("drafts.json")
    for {
      first <- FlowDrafts.inSlot[IO](Some(JsonSlot.file[IO](file)))
      _ <- first.put(broken)
      again <- FlowDrafts.inSlot[IO](Some(JsonSlot.file[IO](file)))
      found <- again.get("approval")
      _ <- IO.blocking { Files.deleteIfExists(file); Files.deleteIfExists(file.getParent) }
    } yield found shouldBe Some(broken)
  }
}
