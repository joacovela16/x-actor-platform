package xplat.flow.engine

import cats.effect.IO
import io.circe.Json
import xplat.core.io.actor.IOActorSystem
import xplat.flow.entity.{EntityCatalog, EntityDef}
import xplat.flow.model._
import xplat.flow.nodes.EntityNodes

/** Flows whose types are entities of the business model. */
class EntityFlowTest extends EngineSpec {

  // `path` of the specs is a state path; the one of the DSL is called `at` here.
  import xplat.flow.entity.dsl.{path => at, _}

  private val Line: EntityDef =
    valueObject("Line")(field("qty", num).min(1), field("price", num), computed("subtotal", num)(at("qty") * at("price")))

  private val Order: EntityDef = entity("Order", key = "id")(
    field("id", str),
    field("lines", listOf(Line)).minItems(1),
    computed("total", num)(sum("lines")(at("subtotal"))),
    field("status", str)
  ).lifecycle("status", initial = "draft")(transition("approve", from = "draft", to = "approved").guard(at("total") > 0))

  private val catalog = EntityCatalog(List(Line, Order))

  private def withEntities[A](f: (FlowEngine[IO], Probe) => IO[A]): IO[A] =
    for {
      journal <- InMemoryJournal.make[IO]
      probe <- Probe.make
      result <- IOActorSystem[IO]("entity-flows").use { system =>
        ActorFlowEngine
          .make[IO](system, journal, List(probe.step) ++ EntityNodes.nodes[IO](IO.pure(catalog)), types = Some(IO.pure(catalog)))
          .flatMap(f(_, probe))
      }
    } yield result

  private val orderInput = OutputDecl("order", path("order"), DataType.Entity("Order"))

  "an input of an entity type is made by it: the instance starts with its defaults and computed fields" in {
    withEntities { (engine, _) =>
      val f = flow("typed", List(step("a")), Nil, inputs = List(orderInput))
      for {
        id <- run(engine, f, json("""{"order": {"id": "O-1", "lines": [{"qty": 2, "price": 10}, {"qty": 1, "price": 5}]}}"""))
        s <- finished(engine, id)
      } yield {
        s.status shouldBe InstanceState.Status.Succeeded
        valueAt(s, "order.total") shouldBe Some(Json.fromInt(25))
        valueAt(s, "order.status") shouldBe Some(Json.fromString("draft"))
      }
    }
  }

  "an input that breaks its entity is refused, saying every problem with its path" in {
    withEntities { (engine, _) =>
      val f = flow("typed", List(step("a")), Nil, inputs = List(orderInput))
      run(engine, f, json("""{"order": {"id": "O-1", "lines": [{"qty": 0, "price": 1}]}}""")).attempt.map {
        case Left(FlowEngine.InvalidInputs(message)) =>
          message shouldBe "input 'order' is not a valid Order: lines.0.qty: must be at least 1"
        case other => fail(s"expected invalid inputs, got $other")
      }
    }
  }

  "a node whose output has an entity type writes it as the entity makes it, or fails" in {
    withEntities { (engine, _) =>
      def writing(value: String) =
        flow("out", List(NodeDef("a", "test.step", json(s"""{"tag": "a", "value": $value}"""), List(OutputDecl("v", path("line"), DataType.Entity("Line"))))), Nil)
      for {
        good <- run(engine, writing("""{"qty": 2, "price": 3}""")).flatMap(finished(engine, _))
        bad <- run(engine, writing("""{"qty": 0, "price": 3}""")).flatMap(finished(engine, _))
      } yield {
        valueAt(good, "line.subtotal") shouldBe Some(Json.fromInt(6))
        status(bad, "a") shouldBe InstanceState.NodeStatus.Failed
        bad.nodes("a").error.get should include("qty: must be at least 1")
      }
    }
  }

  "the entity nodes: a value is checked, then moved to another state" in {
    withEntities { (engine, _) =>
      val validate = NodeDef(
        "check",
        EntityNodes.Validate,
        json("""{"entity": "Order", "path": "raw"}"""),
        EntityNodes.validateDescriptor.defaultOutputs(json("""{"entity": "Order", "path": "order"}"""))
      )
      val approve = NodeDef(
        "approve",
        EntityNodes.Transition,
        json("""{"entity": "Order", "path": "order", "transition": "approve"}"""),
        EntityNodes.transitionDescriptor.defaultOutputs(json("""{"entity": "Order", "path": "order"}"""))
      )
      val f = flow(
        "lifecycle",
        List(validate, approve, step("bad")),
        List(edge("check", "approve", EntityNodes.Valid), edge("check", "bad", EntityNodes.Invalid)),
        inputs = List(OutputDecl("raw", path("raw"), DataType.AnyJson))
      )
      for {
        ok <- run(engine, f, json("""{"raw": {"id": "O-1", "lines": [{"qty": 1, "price": 7}]}}""")).flatMap(finished(engine, _))
        ko <- run(engine, f, json("""{"raw": {"id": "O-2", "lines": []}}""")).flatMap(finished(engine, _))
      } yield {
        valueAt(ok, "order.status") shouldBe Some(Json.fromString("approved"))
        valueAt(ok, "order.total") shouldBe Some(Json.fromInt(7))
        status(ok, "bad") shouldBe InstanceState.NodeStatus.Dead
        status(ko, "approve") shouldBe InstanceState.NodeStatus.Dead
        status(ko, "bad") shouldBe InstanceState.NodeStatus.Succeeded
        valueAt(ko, "orderErrors").flatMap(_.hcursor.downN(0).get[String]("rule").toOption) shouldBe Some("minItems")
        // Through `invalid` the node writes the problems and not the value.
        valueAt(ko, "order") shouldBe None
      }
    }
  }

  "a flow that names an entity that does not exist is not saved" in {
    withEntities { (engine, _) =>
      val f = flow("ghost", List(step("a")), Nil, inputs = List(OutputDecl("x", path("x"), DataType.Entity("Ghost"))))
      engine.store.save(f).map(_.left.map(_.map(_.code)) shouldBe Left(List("unknown-entity")))
    }
  }

  "a simulation makes the inputs the same way" in {
    withEntities { (engine, _) =>
      val f = flow("sim", List(step("a")), Nil, inputs = List(orderInput))
      engine.simulate(f, json("""{"order": {"id": "O-1", "lines": [{"qty": 3, "price": 2}]}}"""), Map.empty).map { trace =>
        path("order.total").get(trace.state) shouldBe Some(Json.fromInt(6))
      }
    }
  }
}
