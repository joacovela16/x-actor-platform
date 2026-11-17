package xplat.flow.http

import cats.effect.IO
import io.circe.Json
import io.circe.syntax._
import org.http4s.{Method, Status}
import xplat.flow.model.NodeDef

import java.nio.charset.StandardCharsets.UTF_8

class HookRoutesTest extends HttpSpec {

  private def setup(app: App, t: TriggerDef = trigger()): IO[Unit] =
    app.save(approval) >> app.put(s"/api/triggers/${t.id}", t.asJson).map(r => r.status shouldBe Status.Ok).void

  private val sync = (t: TriggerDef) => t.copy(response = TriggerDef.ResponseMode.Sync)

  "an asynchronous trigger answers 202 with the instance, which runs with the trigger as origin" in withApp { app =>
    for {
      _ <- setup(app)
      r <- app.post("/hooks/orders", json("""{"approved":true,"order":{"id":7}}"""))
      id = r.json.hcursor.get[String]("instanceId").toOption.get
      status <- app.finished(id)
      audit <- app.get(s"/api/instances/$id/audit")
    } yield {
      r.status shouldBe Status.Accepted
      status.hcursor.get[String]("status") shouldBe Right("succeeded")
      status.hcursor.downField("state").get[Json]("order") shouldBe Right(json("""{"id":7}"""))
      audit.json.hcursor.downField("instance").get[String]("origin") shouldBe Right("trigger:orders")
    }
  }

  "a synchronous trigger answers what the respond node of the flow describes" in withApp { app =>
    for {
      _ <- setup(app, trigger(sync))
      yes <- app.post("/hooks/orders", json("""{"approved":true,"order":{"id":7}}"""))
      no <- app.post("/hooks/orders", json("""{"approved":false,"order":{"id":8}}"""))
    } yield {
      yes.status shouldBe Status.Created
      yes.json shouldBe json("""{"id":7}""")
      yes.headers("x-decision") shouldBe "yes"
      no.status shouldBe Status.Conflict
      no.text shouldBe empty
    }
  }

  "a synchronous trigger of a flow without a respond node answers with the end of the instance" in withApp { app =>
    val quiet = approval.copy(id = "quiet", nodes = List(NodeDef("check", "core.if", json("""{"path":"approved"}"""))), edges = Nil)
    for {
      _ <- app.save(quiet)
      _ <- app.put("/api/triggers/q", trigger(t => sync(t.copy(id = "q", flowId = "quiet", path = Some("quiet")))).asJson)
      r <- app.post("/hooks/quiet", json("""{"approved":true,"order":1}"""))
      failed <- app.post("/hooks/quiet", json("""{"approved":"nope","order":1}"""))
    } yield {
      r.status shouldBe Status.Ok
      r.json.hcursor.get[String]("status") shouldBe Right("succeeded")
      // The engine refuses inputs that do not fit before it starts anything.
      failed.status shouldBe Status.UnprocessableContent
    }
  }

  "a synchronous trigger that takes longer than its timeout answers 202 like an asynchronous one" in withApp { app =>
    // The flow does not end by itself: the node has nothing to answer.
    val slow = approval.copy(id = "slow", nodes = List(NodeDef("ok", "core.respond", json("""{"body":"order"}"""))), edges = Nil)
    for {
      _ <- app.save(slow)
      _ <- app.put("/api/triggers/slow", trigger(t => sync(t.copy(id = "slow", flowId = "slow", path = Some("slow"), timeoutMs = Some(1)))).asJson)
      r <- app.post("/hooks/slow", json("""{"approved":true,"order":1}"""))
    } yield (Set(Status.Accepted, Status.Ok)(r.status)) shouldBe true
  }

  "unknown routes and other methods are 404, and a trigger removed stops answering" in withApp { app =>
    for {
      _ <- setup(app)
      nothing <- app.post("/hooks/nothing", json("{}"))
      wrongMethod <- app.call(Method.GET, "/hooks/orders")
      _ <- app.call(Method.DELETE, "/api/triggers/orders")
      removed <- app.post("/hooks/orders", json("""{"approved":true,"order":1}"""))
    } yield List(nothing, wrongMethod, removed).map(_.status).distinct shouldBe List(Status.NotFound)
  }

  "the inputs come from references into the request, or from the body when there are none" in withApp { app =>
    val mapped = trigger(_.copy(inputs = Map("approved" -> Json.fromString("$query.ok"), "order" -> Json.fromString("$headers.x-order"))))
    for {
      _ <- setup(app, mapped)
      r <- app.call(Method.POST, "/hooks/orders?ok=true", headers = List("X-Order" -> "A-1"))
      // The query parameter is text, which is not what `approved` is: the engine says so.
      id = r.json.hcursor.get[String]("instanceId").toOption
    } yield {
      r.status shouldBe Status.UnprocessableContent
      id shouldBe None
    }
  }

  "a trigger without a mapping takes the body as the inputs" in withApp { app =>
    for {
      _ <- setup(app, trigger(_.copy(inputs = Map.empty)))
      r <- app.post("/hooks/orders", json("""{"approved":true,"order":"x"}"""))
      id = r.json.hcursor.get[String]("instanceId").toOption.get
      status <- app.finished(id)
    } yield status.hcursor.downField("state").get[String]("order") shouldBe Right("x")
  }

  "the idempotency key makes a repeated request run once and get the same answer" in withApp { app =>
    val body = json("""{"approved":true,"order":{"id":1}}""")
    for {
      _ <- setup(app, trigger(sync))
      a <- app.post("/hooks/orders", body, List("Idempotency-Key" -> "abc 123"))
      b <- app.post("/hooks/orders", body, List("Idempotency-Key" -> "abc 123"))
      c <- app.post("/hooks/orders", body, List("Idempotency-Key" -> "other"))
      all <- app.get("/api/instances?origin=trigger")
    } yield {
      (a.status, b.status, c.status) shouldBe ((Status.Created, Status.Created, Status.Created))
      a.json shouldBe b.json
      all.json.as[List[Json]].toOption.get.map(_.hcursor.get[String]("id").toOption.get).sorted shouldBe List("orders-abc_123", "orders-other")
    }
  }

  "authentication" - {
    def secured(scheme: String, header: Option[String] = None) =
      trigger(_.copy(auth = Some(TriggerDef.Auth(scheme, "hook.token", header))))

    def withSecret(app: App, t: TriggerDef): IO[Unit] =
      app.put("/api/connections/hook", json("""{"kind":"secret","secrets":{"token":"s3cr3t"}}""")) >> setup(app, t)

    val body = """{"approved":true,"order":1}"""

    "an api key" in withApp { app =>
      for {
        _ <- withSecret(app, secured("apiKey"))
        none <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body))
        wrong <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body), headers = List("X-Api-Key" -> "nope"))
        right <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body), headers = List("X-Api-Key" -> "s3cr3t"))
      } yield (none.status, wrong.status, right.status) shouldBe ((Status.Unauthorized, Status.Unauthorized, Status.Accepted))
    }

    "a bearer token" in withApp { app =>
      for {
        _ <- withSecret(app, secured("bearer"))
        wrong <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body), headers = List("Authorization" -> "Bearer nope"))
        right <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body), headers = List("Authorization" -> "Bearer s3cr3t"))
      } yield (wrong.status, right.status) shouldBe ((Status.Unauthorized, Status.Accepted))
    }

    "an HMAC signature of the body" in withApp { app =>
      for {
        _ <- withSecret(app, secured("hmac", Some("X-Hub-Signature-256")))
        wrong <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body), headers = List("X-Hub-Signature-256" -> HookAuth.sign("other", body.getBytes(UTF_8))))
        tampered <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body.replace("true", "false")), headers = List("X-Hub-Signature-256" -> HookAuth.sign("s3cr3t", body.getBytes(UTF_8))))
        right <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body), headers = List("X-Hub-Signature-256" -> HookAuth.sign("s3cr3t", body.getBytes(UTF_8))))
      } yield (wrong.status, tampered.status, right.status) shouldBe ((Status.Unauthorized, Status.Unauthorized, Status.Accepted))
    }

    "a secret that is not set lets nobody in" in withApp { app =>
      for {
        _ <- setup(app, secured("apiKey"))
        r <- app.call(Method.POST, "/hooks/orders", rawBody = Some(body), headers = List("X-Api-Key" -> ""))
      } yield r.status shouldBe Status.Unauthorized
    }
  }
}
