package xplat.flow.http

import cats.effect.IO
import io.circe.Json
import org.http4s.Status
import xplat.flow.model._
import xplat.flow.nodes.{IfNode, RespondNode}

/** Moving flows between two servers: export from one, plan and import into the other. */
class BundleTest extends HttpSpec {

  private val keyed = FlowDef("keyed", "Keyed", nodes = List(NodeDef("call", SecretNode.descriptor.kind, json("""{"apiKey":"s3cr3t","note":"n"}"""))))

  private def queueTrigger(id: String, connection: String) =
    TriggerDef(id, "approval", TriggerDef.Kind.Queue, queue = Some("orders"), connection = Some(connection))

  /** A server with two flows (one enabled), an HTTP trigger, two queue triggers and their connections. */
  private def source(app: App): IO[Unit] =
    for {
      _ <- app.save(approval)
      _ <- app.save(keyed)
      _ <- app.deployments.set("approval", true)
      _ <- app.triggers.put(trigger())
      _ <- app.triggers.put(queueTrigger("q-api", "api"))
      _ <- app.triggers.put(queueTrigger("q-vault", "vault"))
      _ <- app.connections.put("api", "http", json("""{"baseUrl":"https://dev.example.com"}"""), Map("token" -> "t0k"))
      _ <- app.connections.put("vault", "secret", Json.obj(), Map("token" -> "v"))
    } yield ()

  private def items(plan: Json): Map[(String, String), String] =
    plan.hcursor.downField("items").as[List[Json]].toOption.get.map { i =>
      val c = i.hcursor
      (c.get[String]("type").toOption.get, c.get[String]("id").toOption.get) -> c.get[String]("action").toOption.get
    }.toMap

  private def strings(plan: Json, field: String): List[String] = plan.hcursor.get[List[String]](field).toOption.get

  "a bundle has no secrets and no revisions, and its text is stable" in
    withApp { app =>
      for {
        _ <- source(app)
        _ <- app.save(approval.copy(name = "Approval v2")) // revision 2
        first <- app.get("/api/bundle")
        second <- app.get("/api/bundle")
      } yield {
        first.status shouldBe Status.Ok
        first.text shouldBe second.text
        first.text should not include "s3cr3t"
        first.text should not include "t0k"
        first.text should not include "\"revision\""
        val b = first.json.hcursor
        b.get[String]("format") shouldBe Right("x-flow-bundle")
        b.downField("connections").as[List[Json]].map(_.map(_.hcursor.get[String]("name").toOption.get)) shouldBe Right(List("api", "vault"))
        b.downField("requires").get[List[String]]("kinds") shouldBe Right(List(IfNode.Kind, RespondNode.Kind, SecretNode.descriptor.kind).sorted)
        b.downField("flows").as[List[Json]].map(_.map(f => f.hcursor.get[Boolean]("enabled").toOption.get)) shouldBe Right(List(true, false))
      }
    }

  "only the flows asked for, with their triggers; a flow that does not exist is a 404" in
    withApp { app =>
      for {
        _ <- source(app)
        some <- app.get("/api/bundle?flows=keyed")
        missing <- app.get("/api/bundle?flows=keyed,nope")
      } yield {
        some.json.hcursor.downField("flows").as[List[Json]].map(_.size) shouldBe Right(1)
        some.json.hcursor.downField("triggers").as[List[Json]] shouldBe Right(Nil)
        missing.status shouldBe Status.NotFound
        missing.text should include("flow 'nope' does not exist")
      }
    }

  "into another server: the plan first, then the import, and importing again changes nothing" in
    withApp { dev =>
      withApp { prod =>
        for {
          _ <- source(dev)
          bundle <- dev.get("/api/bundle").map(_.json)
          plan <- prod.post("/api/bundle?dryRun=true", bundle)
          before <- prod.get("/api/flows").map(_.json)
          imported <- prod.post("/api/bundle", bundle)
          again <- prod.post("/api/bundle?dryRun=true", bundle)
          enabled <- prod.deployments.enabled
          api <- prod.connections.get("api")
          vault <- prod.connections.get("vault")
          keyedDraft <- prod.drafts.get("keyed")
          keyedPublished <- prod.engine.store.get("keyed", None)
          back <- prod.get("/api/bundle")
          devAgain <- dev.get("/api/bundle")
        } yield {
          plan.status shouldBe Status.Ok
          before shouldBe Json.arr()
          items(plan.json) shouldBe Map(
            ("connection", "api") -> "create",
            ("connection", "vault") -> "skip",
            ("flow", "approval") -> "create",
            ("flow", "keyed") -> "draft",
            ("trigger", "orders") -> "create",
            ("trigger", "q-api") -> "create",
            ("trigger", "q-vault") -> "create"
          )
          strings(plan.json, "problems") shouldBe Nil
          strings(plan.json, "warnings").mkString("\n") should (include("'vault' (secret) does not exist here and needs secrets (token)") and
            include("flow 'keyed' lacks secret params here (call.apiKey): it goes in as a draft, to complete and publish"))

          imported.status shouldBe Status.Ok
          enabled shouldBe Set("approval")
          api.map(_.params) shouldBe Some(json("""{"baseUrl":"https://dev.example.com"}"""))
          api.map(_.secretsSet) shouldBe Some(Nil)
          vault shouldBe None
          keyedDraft.map(_.nodes.head.params) shouldBe Some(json("{\"note\":\"n\"}"))
          keyedPublished shouldBe None
          items(again.json).values.toSet shouldBe Set("same", "skip", "draft")
          // The same text for what was published here.
          def approvalOf(j: Json) = j.hcursor.downField("flows").as[List[Json]].toOption.get.find(_.hcursor.downField("flow").get[String]("id").contains("approval"))
          approvalOf(back.json) shouldBe approvalOf(devAgain.json)
          back.json.hcursor.downField("triggers").focus shouldBe devAgain.json.hcursor.downField("triggers").focus
        }
      }
    }

  "an existing connection is never changed, and a flow that did not change gets no new revision" in
    withApp { dev =>
      withApp { prod =>
        for {
          _ <- source(dev)
          bundle <- dev.get("/api/bundle").map(_.json)
          _ <- prod.connections.put("api", "http", json("""{"baseUrl":"https://prod.example.com"}"""), Map.empty)
          _ <- prod.post("/api/bundle", bundle)
          _ <- prod.post("/api/bundle", bundle)
          api <- prod.connections.get("api")
          revisions <- prod.engine.store.revisions("approval")
          plan <- prod.post("/api/bundle?dryRun=true", bundle)
        } yield {
          api.map(_.params) shouldBe Some(json("""{"baseUrl":"https://prod.example.com"}"""))
          revisions.map(_.revision) shouldBe List(1)
          strings(plan.json, "warnings").mkString should include("connection 'api' exists here with other settings")
        }
      }
    }

  "a flow that changed in the bundle gets a new revision and keeps the secrets of the target" in
    withApp { dev =>
      withApp { prod =>
        for {
          _ <- source(dev)
          _ <- prod.save(keyed.copy(nodes = List(NodeDef("call", SecretNode.descriptor.kind, json("""{"apiKey":"prod-key","note":"old"}""")))))
          bundle <- dev.get("/api/bundle").map(_.json)
          plan <- prod.post("/api/bundle?dryRun=true", bundle)
          _ <- prod.post("/api/bundle", bundle)
          saved <- prod.engine.store.get("keyed", None)
        } yield {
          items(plan.json)(("flow", "keyed")) shouldBe "update"
          saved.map(_.revision) shouldBe Some(2)
          saved.map(_.nodes.head.params) shouldBe Some(json("""{"apiKey":"prod-key","note":"n"}"""))
        }
      }
    }

  "a bundle that needs what the server does not have is not imported" in
    withApp { prod =>
      val bundle = json(s"""{"format":"x-flow-bundle","version":1,
        "flows":[{"flow":{"id":"x","name":"X","nodes":[{"id":"a","kind":"nope.thing","params":{}}]},"enabled":true}],
        "connections":[{"name":"k","kind":"kafka","params":{}}]}""")
      for {
        refused <- prod.post("/api/bundle", bundle)
        flows <- prod.get("/api/flows").map(_.json)
        notABundle <- prod.post("/api/bundle", json("""{"format":"other","version":1}"""))
        newer <- prod.post("/api/bundle", json("""{"format":"x-flow-bundle","version":9}"""))
      } yield {
        refused.status shouldBe Status.UnprocessableEntity
        strings(refused.json, "problems") should contain allOf ("this server has no node kind 'nope.thing'", "this server has no connection kind 'kafka'")
        flows shouldBe Json.arr()
        notABundle.status shouldBe Status.BadRequest
        newer.text should include("bundle version 9")
      }
    }

  "as a directory: one file per item, and back to the same bundle" in
    withApp { app =>
      for {
        _ <- source(app)
        text <- app.get("/api/bundle").map(_.text)
      } yield {
        val bundle = io.circe.parser.decode[Bundle](text).toOption.get
        val files = BundleFiles.write(bundle).toOption.get
        files.keySet shouldBe Set(
          "bundle.json",
          "flows/approval.json",
          "flows/keyed.json",
          "triggers/orders.json",
          "triggers/q-api.json",
          "triggers/q-vault.json",
          "connections/api.json",
          "connections/vault.json"
        )
        files("bundle.json") should (include("requires") and not include "flows")
        files("flows/keyed.json") should include("\"enabled\" : false")
        BundleFiles.read(files) shouldBe Right(bundle)
        BundleFiles.read(files - "bundle.json") shouldBe Left("bundle.json is missing")
        BundleFiles.read(files + ("flows/broken.json" -> "{")).left.toOption.get should startWith("flows/broken.json")
        BundleFiles.write(bundle.copy(triggers = List(trigger(_.copy(id = "../x"))))).left.toOption.get should include("cannot be a file name")
      }
    }
}
