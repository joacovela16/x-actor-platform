package xplat.flow.queue

import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.connectors.{Outgoing, Publisher}
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine._
import xplat.flow.model._
import xplat.flow.nodes.Connections

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class PublishNodeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, identity)
  private def path(text: String): StatePath = StatePath.parse(text).toOption.get

  /** Publishers that write to a list, and record how many were opened and closed. */
  private final class Fake(val sent: Ref[IO, List[(String, String, Outgoing[Array[Byte]])]], val opened: Ref[IO, Int], val closed: Ref[IO, Int], fail: Boolean = false)
      extends PublisherFactory[IO] {
    def open(c: Connections.Resolved, target: String, destinationType: String): Resource[IO, Publisher[IO, Array[Byte]]] =
      Resource.make(opened.update(_ + 1).as(new Publisher[IO, Array[Byte]] {
        val destination: String = target
        def publish(m: Outgoing[Array[Byte]]): IO[Unit] =
          if (fail) IO.raiseError(new RuntimeException("broker is down")) else sent.update(_ :+ ((c.name, target + "/" + destinationType, m)))
      }))(_ => closed.update(_ + 1))
  }

  private def fake(fail: Boolean = false): IO[Fake] =
    for { s <- Ref.of[IO, List[(String, String, Outgoing[Array[Byte]])]](Nil); o <- Ref.of[IO, Int](0); c <- Ref.of[IO, Int](0) } yield new Fake(s, o, c, fail)

  private val kafka = Connections.Resolved("kafka-prod", "kafka", json("""{"bootstrapServers":"b:9092"}"""), Map("password" -> "secret"))
  private def connections(known: Connections.Resolved*): Connections[IO] = name => IO.pure(known.find(_.name == name))

  private val params = """{"connection":"kafka-prod","destination":"orders","payload":"order","key":"order.id","headers":{"source":"flow"}}"""
  private val state = """{"order":{"id":"o-1","total":10}}"""
  private def input(p: String = params, s: String = state) = NodeInput(json(p), json(s), Json.obj(), "inst-1", "pub")

  private def withNode[A](f: Fake, known: Connections.Resolved*)(use: PublishNode[IO] => IO[A]): IO[A] =
    PublisherPool.make[IO](f).use(pool => use(new PublishNode[IO](connections(known: _*), pool)))

  "it publishes the value as JSON with the key, the headers and the idempotency header" in {
    for {
      f <- fake()
      out <- withNode(f, kafka)(_.run(input()))
      sent <- f.sent.get
    } yield {
      out.failed shouldBe false
      out.port shouldBe Port.Out
      sent should have size 1
      val (connection, destination, message) = sent.head
      connection shouldBe "kafka-prod"
      destination shouldBe "orders/queue"
      json(new String(message.value, UTF_8)) shouldBe json("""{"id":"o-1","total":10}""")
      message.key shouldBe Some("o-1")
      message.headers shouldBe Map("source" -> "flow", "x-idempotency-key" -> "inst-1:pub")
      // The audit learns where it went, never the payload or the credentials.
      out.info.keySet shouldBe Set("connection", "destination", "key", "x-idempotency-key")
      out.info.values.map(_.noSpaces).mkString should not include "secret"
    }
  }

  "the same node of the same instance sends the same idempotency key every time" in {
    for {
      f <- fake()
      _ <- withNode(f, kafka)(n => n.run(input()) >> n.run(input()))
      sent <- f.sent.get
    } yield sent.map(_._3.headers("x-idempotency-key")).distinct shouldBe List("inst-1:pub")
  }

  "text sends a string as it is" in {
    for {
      f <- fake()
      _ <- withNode(f, kafka)(_.run(input("""{"connection":"kafka-prod","destination":"t","payload":"msg","format":"text"}""", """{"msg":"hello"}""")))
      sent <- f.sent.get
    } yield new String(sent.head._3.value, UTF_8) shouldBe "hello"
  }

  "problems fail through the error port and nothing is sent" in {
    for {
      f <- fake()
      noValue <- withNode(f, kafka)(_.run(input(s = "{}")))
      noConnection <- withNode(f)(_.run(input()))
      notText <- withNode(f, kafka)(_.run(input("""{"connection":"kafka-prod","destination":"t","payload":"order","format":"text"}""")))
      badKey <- withNode(f, kafka)(_.run(input(s = """{"order":{"id":{"x":1}}}""")))
      badParams <- withNode(f, kafka)(_.run(input("""{"destination":"t"}""")))
      sent <- f.sent.get
    } yield {
      List(noValue, noConnection, notText, badKey, badParams).foreach { o => o.failed shouldBe true; o.port shouldBe Port.Error }
      noConnection.error.get should include("does not exist")
      sent shouldBe empty
    }
  }

  "a broker that fails is a failed node, not an exception" in {
    for {
      f <- fake(fail = true)
      out <- withNode(f, kafka)(_.run(input()))
    } yield {
      out.port shouldBe Port.Error
      out.error.get should (include("cannot publish to orders") and include("broker is down"))
    }
  }

  "a simulation says what it would send and does not connect" in {
    for {
      f <- fake()
      out <- withNode(f, kafka)(n => IO.pure(n.simulated(input())))
      opened <- f.opened.get
    } yield {
      opened shouldBe 0
      val info = out.get.info
      info("destination") shouldBe Json.fromString("orders")
      info("payload") shouldBe json("""{"id":"o-1","total":10}""")
      info("key") shouldBe Json.fromString("o-1")
      info("headers").hcursor.get[String]("x-idempotency-key") shouldBe Right("inst-1:pub")
    }
  }

  "the pool opens one publisher per destination, opens another when the connection changes, and closes all at the end" in {
    for {
      f <- fake()
      _ <- PublisherPool.make[IO](f).use { pool =>
        for {
          a <- pool.use(kafka, "orders", "queue")(IO.pure)
          b <- pool.use(kafka, "orders", "queue")(IO.pure)
          _ <- pool.use(kafka, "payments", "queue")(IO.pure)
          _ <- f.opened.get.map(_ shouldBe 2)
          _ <- IO(a should be theSameInstanceAs b)
          _ <- pool.use(kafka.copy(params = json("""{"bootstrapServers":"other:9092"}""")), "orders", "queue")(IO.pure)
          _ <- f.opened.get.map(_ shouldBe 3)
          // The old one is not closed at once: it is idle, and the pool closes it when it expires or at the end.
          _ <- f.closed.get.map(_ shouldBe 0)
        } yield ()
      }
      closed <- f.closed.get
    } yield closed shouldBe 3
  }

  "the connection kinds describe their secrets, and the broker settings are built from them" in IO {
    QueueKinds.all.map(_.kind) shouldBe List("kafka", "activemq", "redis")
    QueueKinds.all.flatMap(_.config.fields.filter(_.secret).map(_.name)) shouldBe List("password", "password", "password")
    val props = QueueKinds.kafkaProperties(
      Connections.Resolved("k", "kafka", json("""{"bootstrapServers":"b","securityProtocol":"SASL_SSL","username":"me","properties":{"linger.ms":5}}"""), Map("password" -> "p\"w"))
    )
    props("security.protocol") shouldBe "SASL_SSL"
    props("sasl.jaas.config") shouldBe """org.apache.kafka.common.security.plain.PlainLoginModule required username="me" password="p\"w";"""
    props("linger.ms") shouldBe "5"
    QueueKinds.redisUri(Connections.Resolved("r", "redis", json("""{"uri":"redis://h:6379"}"""), Map("password" -> "p@ss"))) shouldBe Right("redis://:p%40ss@h:6379")
    QueueKinds.redisUri(Connections.Resolved("r", "redis", json("""{"uri":"redis://h:6379"}"""), Map.empty)) shouldBe Right("redis://h:6379")
  }

  "validation: the payload must be declared upstream, and the node is a Write" in IO {
    val flow = FlowDef(
      "f", "F",
      inputs = List(OutputDecl("order", path("order"), DataType.Obj(Map("id" -> DataType.Str)))),
      nodes = List(NodeDef("pub", PublishNode.Kind, json(params)))
    )
    FlowValidator.validate(flow, QueueNodes.catalog) shouldBe Nil
    FlowValidator.validate(flow.copy(inputs = Nil), QueueNodes.catalog).map(_.code) shouldBe List("state-not-declared", "state-not-declared")
    FlowValidator.validate(flow.copy(nodes = List(NodeDef("pub", PublishNode.Kind, json("""{"destination":"x"}""")))), QueueNodes.catalog).map(_.code) shouldBe List("invalid-params")
    PublishNode.descriptor.sideEffects shouldBe SideEffects.Write
  }

  "in the engine: a simulation shows the message in the audit without sending it, and a real run publishes" in {
    val flow = FlowDef(
      "pub-flow", "Publish",
      inputs = List(OutputDecl("order", path("order"), DataType.Obj(Map("id" -> DataType.Str)))),
      nodes = List(NodeDef("pub", PublishNode.Kind, json(params)))
    )
    IOActorSystem[IO]("queue-test").use { system =>
      for {
        f <- fake()
        result <- QueueNodes.resource[IO](connections(kafka), f).use { nodes =>
          for {
            journal <- InMemoryJournal.make[IO]
            engine <- ActorFlowEngine.make[IO](system, journal, extraNodes = nodes)
            saved <- engine.store.save(flow)
            trace <- engine.simulate(flow, json(state), Map.empty)
            opened <- f.opened.get
            id <- engine.start("pub-flow", None, json(state), None)
            _ <- engine.events(id).compile.drain
            audit <- engine.audit(id)
          } yield (saved.isRight, trace, opened, audit)
        }
        sent <- f.sent.get
      } yield {
        val (saved, trace, openedInSimulation, audit) = result
        saved shouldBe true
        trace.status.toString should include("ucceeded")
        val simulated = trace.audit.collect { case n: AuditEntry.NodeFinished => n }.head
        simulated.info("payload") shouldBe json("""{"id":"o-1","total":10}""")
        simulated.simulation.map(_.outcome) shouldBe Some("simulated")
        openedInSimulation shouldBe 0
        sent should have size 1
        val real = audit.get.entries.collect { case n: AuditEntry.NodeFinished => n }.head
        real.info.get("payload") shouldBe None
        real.info("x-idempotency-key").asString.get should endWith(":pub")
      }
    }
  }
}
