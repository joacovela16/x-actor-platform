package xplat.flow.http

import io.circe.Json
import io.circe.syntax._
import org.http4s.{Method, Status}

class AdminRoutesTest extends HttpSpec {

  "triggers" - {
    "are saved, listed, read and deleted, with the flow checked" in withApp { app =>
      for {
        noFlow <- app.put("/api/triggers/orders", trigger().asJson)
        _ <- app.save(approval)
        saved <- app.put("/api/triggers/orders", trigger().asJson)
        list <- app.get("/api/triggers")
        one <- app.get("/api/triggers/orders")
        deleted <- app.call(Method.DELETE, "/api/triggers/orders")
        gone <- app.get("/api/triggers/orders")
      } yield {
        noFlow.status shouldBe Status.UnprocessableContent
        saved.status shouldBe Status.Ok
        list.json.as[List[Json]].toOption.get.map(_.hcursor.get[String]("id").toOption.get) shouldBe List("orders")
        one.json.hcursor.get[String]("path") shouldBe Right("orders")
        deleted.status shouldBe Status.NoContent
        gone.status shouldBe Status.NotFound
      }
    }

    "that are badly formed or that clash with another are refused" in withApp { app =>
      for {
        _ <- app.save(approval)
        _ <- app.put("/api/triggers/orders", trigger().asJson)
        clash <- app.put("/api/triggers/other", trigger(_.copy(id = "other")).asJson)
        noPath <- app.put("/api/triggers/p", trigger(_.copy(path = None)).asJson)
        cron <- app.put("/api/triggers/c", trigger(_.copy(kind = TriggerDef.Kind.Cron)).asJson)
        otherMethod <- app.put("/api/triggers/get", trigger(_.copy(method = Some("GET"))).asJson)
        weird <- app.put("/api/triggers/x", json("""{"id":"x","flowId":"approval","kind":"carrier-pigeon"}"""))
      } yield {
        clash.status shouldBe Status.UnprocessableContent
        clash.text should include("already serves")
        noPath.status shouldBe Status.UnprocessableContent
        cron.status shouldBe Status.UnprocessableContent
        otherMethod.status shouldBe Status.Ok
        weird.status shouldBe Status.BadRequest
      }
    }
  }

  "connections" - {
    "have kinds, and the secrets are write-only" in withApp { app =>
      for {
        kinds <- app.get("/api/connection-kinds")
        saved <- app.put("/api/connections/api", json("""{"kind":"http","params":{"baseUrl":"https://x.test"},"secrets":{"token":"s3cr3t"}}"""))
        list <- app.get("/api/connections")
        // Saving again without the secret keeps it; an empty one does not change it either.
        again <- app.put("/api/connections/api", json("""{"kind":"http","params":{"baseUrl":"https://y.test"},"secrets":{"token":""}}"""))
        stored <- app.secrets.get("api.token")
        deleted <- app.call(Method.DELETE, "/api/connections/api")
        after <- app.secrets.names("api.")
      } yield {
        kinds.json.as[List[Json]].toOption.get.map(_.hcursor.get[String]("kind").toOption.get) shouldBe List("secret", "http")
        saved.json shouldBe json("""{"name":"api","kind":"http","params":{"baseUrl":"https://x.test"},"secretsSet":["token"],"env":[],"envMissing":[]}""")
        list.text should not include "s3cr3t"
        again.json.hcursor.get[List[String]]("secretsSet") shouldBe Right(List("token"))
        again.text should not include "s3cr3t"
        stored shouldBe Some("s3cr3t")
        deleted.status shouldBe Status.NoContent
        after shouldBe Nil
      }
    }

    "are refused when they do not fit their kind" in withApp { app =>
      for {
        unknownKind <- app.put("/api/connections/a", json("""{"kind":"nope"}"""))
        missingSecret <- app.put("/api/connections/b", json("""{"kind":"secret"}"""))
        missingParam <- app.put("/api/connections/c", json("""{"kind":"http","secrets":{"token":"x"}}"""))
        notSecret <- app.put("/api/connections/d", json("""{"kind":"http","params":{"baseUrl":"u"},"secrets":{"baseUrl":"x"}}"""))
        name <- app.put("/api/connections/bad%20name", json("""{"kind":"secret","secrets":{"token":"x"}}"""))
        list <- app.get("/api/connections")
      } yield {
        List(unknownKind, missingSecret, missingParam, notSecret, name).map(_.status).distinct shouldBe List(Status.UnprocessableContent)
        list.json shouldBe Json.arr()
      }
    }
  }
}
