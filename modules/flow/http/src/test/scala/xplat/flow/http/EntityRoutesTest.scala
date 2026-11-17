package xplat.flow.http

import cats.effect.IO
import cats.syntax.functor._
import io.circe.Json
import io.circe.syntax._
import org.http4s.Status
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine.{ActorFlowEngine, Deployments, InMemoryJournal}
import xplat.flow.entity.EntityDef
import xplat.flow.model._
import xplat.flow.nodes.{ConnectionKind, EntityNodes, IfNode}

class EntityRoutesTest extends HttpSpec {

  import xplat.flow.entity.dsl._

  /** An entity of code, which the API shows but does not let change. */
  private val Currency: EntityDef = entity("Currency", key = "code")(field("code", str).pattern("^[A-Z]{3}$"), field("decimals", num).default(2))

  private val customer = json("""{
    "kind": "root", "key": "id",
    "fields": [
      {"name": "id", "type": {"type": "str"}},
      {"name": "name", "type": {"type": "str"}, "constraints": {"minLength": 1}},
      {"name": "vip", "type": {"type": "bool"}, "default": false}
    ]
  }""")

  private val order = json("""{
    "kind": "root", "key": "id",
    "fields": [
      {"name": "id", "type": {"type": "str"}},
      {"name": "customer", "type": {"type": "ref", "entity": "Customer"}},
      {"name": "amount", "type": {"type": "num"}, "constraints": {"min": 0}},
      {"name": "status", "type": {"type": "str"}}
    ],
    "states": {"field": "status", "initial": "new", "states": ["new", "paid"],
               "transitions": [{"name": "pay", "from": ["new"], "to": "paid", "guard": {"gt": [{"path": "amount"}, 0]}}]}
  }""")

  /** The routes with a store of entities, which the engine also uses for its types and nodes. */
  private def withEntities[A](f: (App, EntityStore[IO]) => IO[A]): IO[A] =
    IOActorSystem[IO]("entity-http").use { system =>
      for {
        store <- EntityStore.make[IO](builtIn = List(EntityStore.Fixed(Currency, EntityStore.Source.Code)))
        journal <- InMemoryJournal.make[IO]
        engine <- ActorFlowEngine.make[IO](system, journal, extraNodes = EntityNodes.nodes[IO](store.catalog), types = Some(store.catalog.widen[Types]))
        triggers <- TriggerRegistry.make[IO]()
        secrets <- SecretStore.make[IO]()
        connections <- ConnectionStore.make[IO](ConnectionKind.defaults, secrets)
        deployments <- Deployments.inMemory[IO]
        drafts <- FlowDrafts.inMemory[IO]
        app = new App(engine, triggers, connections, secrets, deployments, drafts, Some(store))
        result <- f(app, store)
      } yield result
    }

  private def errorCodes(r: Response): List[String] = r.json.hcursor.downField("errors").as[List[Json]].toOption.get.flatMap(_.hcursor.get[String]("code").toOption)

  "entities are saved by name, with revisions, and listed with where they come from" in {
    withEntities { (app, _) =>
      for {
        first <- app.put("/api/entities/Customer", customer)
        second <- app.put("/api/entities/Customer", customer)
        list <- app.get("/api/entities")
        one <- app.get("/api/entities/Customer")
        code <- app.get("/api/entities/Currency")
        revisions <- app.get("/api/entities/Customer/revisions")
      } yield {
        first.status shouldBe Status.Ok
        first.json.hcursor.downField("entity").get[Int]("revision") shouldBe Right(1)
        second.json.hcursor.downField("entity").get[Int]("revision") shouldBe Right(2)
        list.json.as[List[Json]].toOption.get.map(e => e.hcursor.get[String]("name").toOption.get -> e.hcursor.get[String]("source").toOption.get) shouldBe
          List("Currency" -> "code", "Customer" -> "store")
        one.json.hcursor.get[String]("source") shouldBe Right("store")
        code.json.as[EntityDef].map(_.fields.map(_.name)) shouldBe Right(List("code", "decimals"))
        revisions.json.as[List[Json]].map(_.size) shouldBe Right(2)
      }
    }
  }

  "an entity that is not valid is refused with every problem, and one that does not decode says where" in {
    withEntities { (app, _) =>
      for {
        orphan <- app.put("/api/entities/Order", order)
        broken <- app.put("/api/entities/X", json("""{"key": "id", "fields": [{"name": "id", "type": {"type": "str"}, "computed": {"sum": 1}}]}"""))
        validated <- app.post("/api/entities/validate", json("""{"name": "Y", "fields": [{"name": "a", "type": {"type": "num"}, "constraints": {"pattern": "x"}}]}"""))
      } yield {
        orphan.status shouldBe Status.UnprocessableContent
        errorCodes(orphan) shouldBe List("unknown-entity")
        broken.status shouldBe Status.UnprocessableContent
        errorCodes(broken) shouldBe List("invalid-entity")
        validated.status shouldBe Status.Ok
        errorCodes(validated) should contain allOf ("missing-key", "invalid-constraint")
      }
    }
  }

  "entities of code cannot be changed or deleted" in {
    withEntities { (app, _) =>
      for {
        put <- app.put("/api/entities/Currency", Currency.asJson)
        delete <- app.delete("/api/entities/Currency")
      } yield {
        put.status shouldBe Status.Conflict
        delete.status shouldBe Status.Conflict
      }
    }
  }

  "a value is tried against an entity, saved or not, and against its transitions" in {
    withEntities { (app, _) =>
      for {
        _ <- app.put("/api/entities/Customer", customer)
        _ <- app.put("/api/entities/Order", order)
        good <- app.post("/api/entities/Order/check", json("""{"value": {"id": "1", "customer": "c", "amount": 5}}"""))
        bad <- app.post("/api/entities/Order/check", json("""{"value": {"id": "1", "customer": "c", "amount": -1}}"""))
        paid <- app.post("/api/entities/Order/check", json("""{"value": {"id": "1", "customer": "c", "amount": 5}, "transition": "pay"}"""))
        draft <- app.post("/api/entities/Draft/check", json("""{"value": {"n": 1}, "entity": {"kind": "value", "fields": [{"name": "n", "type": {"type": "num"}, "constraints": {"max": 0}}]}}"""))
        missing <- app.post("/api/entities/Nope/check", json("""{"value": {}}"""))
      } yield {
        good.json.hcursor.get[Boolean]("ok") shouldBe Right(true)
        good.json.hcursor.downField("value").get[String]("status") shouldBe Right("new")
        bad.json.hcursor.downField("problems").downN(0).get[String]("path") shouldBe Right("amount")
        paid.json.hcursor.downField("value").get[String]("status") shouldBe Right("paid")
        draft.json.hcursor.downField("problems").downN(0).get[String]("rule") shouldBe Right("max")
        missing.status shouldBe Status.NotFound
      }
    }
  }

  "flows use entities as types, and the API says which flows a change breaks and which use an entity" in {
    withEntities { (app, _) =>
      val typed = FlowDef(
        "orders",
        "Orders",
        inputs = List(OutputDecl("order", path("order"), DataType.Entity("Order")), OutputDecl("paid", path("paid"), DataType.Bool)),
        nodes = List(NodeDef("check", IfNode.Kind, json("""{"path": "paid"}""")))
      )
      val reading = FlowDef(
        "vips",
        "Vips",
        inputs = List(OutputDecl("order", path("order"), DataType.Entity("Vip"))),
        nodes = List(NodeDef("check", IfNode.Kind, json("""{"path": "order.vip"}""")))
      )
      for {
        before <- app.save(typed)
        _ <- app.put("/api/entities/Customer", customer)
        _ <- app.put("/api/entities/Order", order)
        saved <- app.save(typed)
        start <- app.post("/api/flows/orders/instances", json("""{"inputs": {"order": {"id": "1", "customer": "c", "amount": -3}, "paid": true}}"""))
        inUse <- app.delete("/api/entities/Order")
        // A change that leaves a field out breaks the flows that read it.
        _ <- app.put("/api/entities/Vip", json("""{"kind": "value", "fields": [{"name": "vip", "type": {"type": "bool"}}]}"""))
        _ <- app.save(reading)
        changed <- app.put("/api/entities/Vip", json("""{"kind": "value", "fields": [{"name": "level", "type": {"type": "num"}}]}"""))
      } yield {
        before.status shouldBe Status.UnprocessableContent
        errorCodes(before) shouldBe List("unknown-entity")
        saved.status shouldBe Status.Ok
        start.status shouldBe Status.UnprocessableContent
        start.text should include("amount: must be at least 0")
        inUse.status shouldBe Status.Conflict
        inUse.text should include("orders")
        changed.status shouldBe Status.Ok
        changed.json.hcursor.downField("affected").downN(0).get[String]("flowId") shouldBe Right("vips")
      }
    }
  }

  "an entity that another one uses cannot be deleted; one that nothing uses can" in {
    withEntities { (app, _) =>
      for {
        _ <- app.put("/api/entities/Customer", customer)
        _ <- app.put("/api/entities/Order", order)
        blocked <- app.delete("/api/entities/Customer")
        gone <- app.delete("/api/entities/Order")
        missing <- app.delete("/api/entities/Order")
      } yield {
        blocked.status shouldBe Status.Conflict
        blocked.text should include("Order")
        gone.status shouldBe Status.NoContent
        missing.status shouldBe Status.NotFound
      }
    }
  }
}
