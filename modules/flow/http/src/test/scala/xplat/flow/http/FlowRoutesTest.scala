package xplat.flow.http

import cats.effect.IO
import io.circe.Json
import io.circe.syntax._
import org.http4s.{Method, Status}
import xplat.flow.model._
import xplat.flow.nodes.IfNode

class FlowRoutesTest extends HttpSpec {

  "catalog" - {
    "lists the kinds of node with their ports, and describes a configuration" in withApp { app =>
      for {
        catalog <- app.get("/api/catalog")
        describe <- app.post("/api/catalog/core.if/describe", json("""{"path":"a.b"}"""))
        unknown <- app.post("/api/catalog/nope/describe", json("{}"))
      } yield {
        catalog.status shouldBe Status.Ok
        val entries = catalog.json.asArray.get
        entries.map(_.hcursor.get[String]("kind").toOption.get) should contain allOf ("core.if", "core.subflow", "core.respond", "test.secret")
        val ifEntry = entries.find(_.hcursor.get[String]("kind").contains("core.if")).get
        ifEntry.hcursor.downField("ports").as[List[Json]].toOption.get.map(_.hcursor.get[String]("name").toOption.get) shouldBe List("then", "else", "error")
        describe.json.hcursor.downField("reads").downArray.get[String]("path") shouldBe Right("a.b")
        describe.json.hcursor.get[List[String]]("problems") shouldBe Right(Nil)
        unknown.status shouldBe Status.NotFound
      }
    }
  }

  "flows" - {
    "saving validates: an invalid flow is a 422 with every error, a valid one a new revision" in withApp { app =>
      val bad = FlowDef("bad", "Bad", List(NodeDef("a", "nope"), NodeDef("b", IfNode.Kind, json("""{"path":"x"}"""))), List(Edge("a", "b")))
      for {
        invalid <- app.save(bad)
        first <- app.save(approval)
        second <- app.save(approval)
        latest <- app.get("/api/flows/approval")
        old <- app.get("/api/flows/approval?revision=1")
        list <- app.get("/api/flows")
        missing <- app.get("/api/flows/ghost")
        badRevision <- app.get("/api/flows/approval?revision=x")
      } yield {
        invalid.status shouldBe Status.UnprocessableContent
        invalid.json.hcursor.downField("errors").as[List[Json]].toOption.get.map(_.hcursor.get[String]("code").toOption.get).sorted shouldBe List("state-not-declared", "unknown-kind")
        first.json.hcursor.get[Int]("revision") shouldBe Right(1)
        second.json.hcursor.get[Int]("revision") shouldBe Right(2)
        latest.json.hcursor.get[Int]("revision") shouldBe Right(2)
        old.json.hcursor.get[Int]("revision") shouldBe Right(1)
        list.json shouldBe json("""[{"id":"approval","name":"Approval","revision":2,"enabled":false,"draft":false}]""")
        missing.status shouldBe Status.NotFound
        badRevision.status shouldBe Status.BadRequest
      }
    }

    "the id of the path is the id of the flow, and PUT creates it" in withApp { app =>
      app.put("/api/flows/other", approval.asJson).flatMap { r =>
        app.get("/api/flows/other").map { g =>
          r.status shouldBe Status.Ok
          g.json.hcursor.get[String]("id") shouldBe Right("other")
        }
      }
    }

    "positions of the nodes survive" in withApp { app =>
      val withLayout = approval.copy(layout = Map("check" -> Position(10, 20)))
      for {
        _ <- app.save(withLayout)
        got <- app.get("/api/flows/approval")
      } yield got.json.hcursor.downField("layout").downField("check").get[Double]("x") shouldBe Right(10.0)
    }

    "validate always answers 200 with the errors" in withApp { app =>
      for {
        ok <- app.post("/api/flows/validate", approval.asJson)
        bad <- app.post("/api/flows/validate", approval.copy(edges = List(Edge("check", "ghost"))).asJson)
        malformed <- app.post("/api/flows/validate", json("""{"id":1}"""))
      } yield {
        ok.status shouldBe Status.Ok
        ok.json shouldBe json("""{"errors":[]}""")
        bad.status shouldBe Status.Ok
        bad.json.hcursor.downField("errors").downArray.get[String]("code") shouldBe Right("unknown-node")
        malformed.status shouldBe Status.BadRequest
      }
    }

    "deleting removes the flow" in withApp { app =>
      for {
        _ <- app.save(approval)
        deleted <- app.call(Method.DELETE, "/api/flows/approval")
        again <- app.call(Method.DELETE, "/api/flows/approval")
        gone <- app.get("/api/flows/approval")
      } yield (deleted.status, again.status, gone.status) shouldBe ((Status.NoContent, Status.NotFound, Status.NotFound))
    }

    "the secrets of a node are not returned, and saving without them keeps them" in withApp { app =>
      val secret = FlowDef("s", "S", List(NodeDef("n", "test.secret", json("""{"apiKey":"abc","note":"hi"}"""))))
      for {
        saved <- app.save(secret)
        got <- app.get("/api/flows/s")
        // The UI sends back what it got, which has no secret.
        resaved <- app.put("/api/flows/s", got.json)
        kept <- app.engine.store.get("s", None)
      } yield {
        saved.status shouldBe Status.Ok
        saved.text should not include "abc"
        got.json.hcursor.downField("nodes").downArray.downField("params").as[Json] shouldBe Right(json("""{"note":"hi"}"""))
        resaved.status shouldBe Status.Ok
        kept.get.nodes.head.params shouldBe json("""{"apiKey":"abc","note":"hi"}""")
        kept.get.revision shouldBe 2
      }
    }
  }

  "instances" - {
    "start, status with skipped nodes, audit, search and retry" in withApp { app =>
      for {
        _ <- app.save(approval)
        started <- app.post("/api/flows/approval/instances", json("""{"inputs":{"approved":true,"order":{"id":7}}}"""))
        id = started.json.hcursor.get[String]("instanceId").toOption.get
        status <- app.finished(id)
        audit <- app.get(s"/api/instances/$id/audit")
        search <- app.get("/api/instances?flowId=approval&status=succeeded&origin=manual&limit=10")
        none <- app.get("/api/instances?flowId=other")
        retried <- app.call(Method.POST, s"/api/instances/$id/retry")
        again <- app.finished(id)
        unknown <- app.get("/api/instances/nope")
        cancelled <- app.call(Method.POST, s"/api/instances/$id/cancel")
        cancelledUnknown <- app.call(Method.POST, "/api/instances/nope/cancel")
        afterCancel <- app.finished(id)
      } yield {
        // An instance that has ended is not changed by cancelling it; one that does not exist is not found.
        cancelled.status shouldBe Status.Accepted
        cancelledUnknown.status shouldBe Status.NotFound
        afterCancel.hcursor.get[String]("status") shouldBe Right("succeeded")
        started.status shouldBe Status.Accepted
        status.hcursor.get[String]("status") shouldBe Right("succeeded")
        status.hcursor.downField("nodes").downField("ok").get[String]("status") shouldBe Right("succeeded")
        status.hcursor.downField("nodes").downField("no").get[String]("status") shouldBe Right("skipped")
        audit.json.hcursor.downField("instance").get[String]("origin") shouldBe Right("manual")
        audit.json.hcursor.downField("entries").as[List[Json]].toOption.get.map(_.hcursor.get[String]("kind").toOption.get) should contain allOf ("instance-started", "node-finished", "edge-dead", "instance-finished")
        search.json.as[List[Json]].toOption.get.map(_.hcursor.get[String]("id").toOption.get) shouldBe List(id)
        none.json shouldBe Json.arr()
        retried.status shouldBe Status.Accepted
        retried.json.hcursor.get[String]("instanceId") shouldBe Right(id)
        again.hcursor.get[String]("status") shouldBe Right("succeeded")
        unknown.status shouldBe Status.NotFound
      }
    }

    "a flow that does not exist, inputs that do not fit and filters that are not valid are client errors" in withApp { app =>
      for {
        _ <- app.save(approval)
        ghost <- app.post("/api/flows/ghost/instances", json("{}"))
        inputs <- app.post("/api/flows/approval/instances", json("""{"inputs":{"approved":"yes"}}"""))
        status <- app.get("/api/instances?status=maybe")
        time <- app.get("/api/instances?from=yesterday")
        iso <- app.get("/api/instances?from=2020-01-01T00:00:00Z&to=2099-01-01T00:00:00Z")
      } yield {
        ghost.status shouldBe Status.NotFound
        inputs.status shouldBe Status.UnprocessableContent
        status.status shouldBe Status.BadRequest
        time.status shouldBe Status.BadRequest
        iso.status shouldBe Status.Ok
      }
    }

    "an instance id that is given makes the start idempotent" in withApp { app =>
      val body = json("""{"instanceId":"fixed","inputs":{"approved":true,"order":1}}""")
      for {
        _ <- app.save(approval)
        a <- app.post("/api/flows/approval/instances", body)
        b <- app.post("/api/flows/approval/instances", body)
        _ <- app.finished("fixed")
        all <- app.get("/api/instances")
      } yield {
        a.json shouldBe b.json
        all.json.as[List[Json]].toOption.get should have size 1
      }
    }

    "events are an SSE stream with a replay of the audit, ending when the instance does" in withApp { app =>
      for {
        _ <- app.save(approval)
        started <- app.post("/api/flows/approval/instances", json("""{"inputs":{"approved":false,"order":1}}"""))
        id = started.json.hcursor.get[String]("instanceId").toOption.get
        _ <- app.finished(id)
        events <- app.get(s"/api/instances/$id/events")
        missing <- app.get("/api/instances/nope/events")
      } yield {
        events.status shouldBe Status.Ok
        events.headers("content-type") should include("text/event-stream")
        val data = events.text.linesIterator.filter(_.startsWith("data:")).map(l => json(l.stripPrefix("data:").trim)).toList
        val types = data.map(_.hcursor.get[String]("type").toOption.get)
        types.head shouldBe "instance-started"
        types.last shouldBe "instance-finished"
        data.map(_.hcursor.get[Long]("seq").toOption.get) shouldBe data.indices.map(_.toLong).toList
        missing.status shouldBe Status.NotFound
      }
    }
  }

  "tests" - {
    "simulate the saved flow or a draft, with the mocks the UI writes" in withApp { app =>
      val body = json("""{"inputs":{"approved":true,"order":1},"mocks":{"ok":{"values":{},"port":"out"}}}""")
      val draft = json("""{"inputs":{"approved":false,"order":1}}""").deepMerge(Json.obj("flow" -> approval.copy(name = "Draft").asJson))
      for {
        _ <- app.save(approval)
        saved <- app.post("/api/flows/approval/test", body)
        unsaved <- app.post("/api/flows/new-draft/test", draft)
        ghost <- app.post("/api/flows/ghost/test", json("{}"))
        badMock <- app.post("/api/flows/approval/test", json("""{"mocks":{"ok":{"port":1}}}"""))
      } yield {
        saved.status shouldBe Status.Ok
        saved.json.hcursor.get[String]("status") shouldBe Right("succeeded")
        val steps = saved.json.hcursor.downField("steps").as[List[Json]].toOption.get
        steps.map(_.hcursor.get[String]("nodeId").toOption.get) should contain("ok")
        unsaved.status shouldBe Status.Ok
        ghost.status shouldBe Status.NotFound
        badMock.status shouldBe Status.BadRequest
      }
    }
  }
}
