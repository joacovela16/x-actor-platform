package xplat.flow.queue

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.connectors.{Incoming, RunningConsumer, Sink}
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine._
import xplat.flow.http.{TriggerDef, TriggerRegistry, TriggerSources}
import xplat.flow.model._
import xplat.flow.nodes.{Connections, IfNode}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class QueueTriggersTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, identity)

  /** Consumers that do not connect anywhere: the test pushes the messages into their sinks. */
  private final class FakeConsumers(sinks: Ref[IO, Map[String, Sink[IO, Array[Byte]]]], val starts: Ref[IO, List[String]]) extends ConsumerFactory[IO] {
    def consume(c: Connections.Resolved, t: TriggerDef, sink: Sink[IO, Array[Byte]]): Resource[IO, RunningConsumer[IO]] =
      Resource
        .make(sinks.update(_ + (t.id -> sink)) >> starts.update(_ :+ s"${t.id}@${c.name}"))(_ => sinks.update(_ - t.id))
        .as(new RunningConsumer[IO] { def stopped: IO[Unit] = IO.never })

    def running: IO[Set[String]] = sinks.get.map(_.keySet)

    private def sinkOf(id: String): IO[Sink[IO, Array[Byte]]] =
      sinks.get.flatMap(_.get(id).fold(IO.sleep(10.millis) >> sinkOf(id))(IO.pure)).timeout(5.seconds)

    def push(id: String, body: String, position: String, headers: Map[String, String] = Map.empty, key: Option[String] = None): IO[Unit] =
      sinkOf(id).flatMap(_.deliver(Incoming("orders", key, body.getBytes(UTF_8), headers, position)))

    def awaitStopped(id: String): IO[Unit] = running.flatMap(r => if (r(id)) IO.sleep(10.millis) >> awaitStopped(id) else IO.unit).timeout(5.seconds)
  }

  private val kafka = Connections.Resolved("kafka-prod", "kafka", json("""{"bootstrapServers":"b:9092"}"""), Map.empty)

  // approved -> if (then / else)
  private val flow = FlowDef(
    "approve", "Approve",
    inputs = List(OutputDecl("approved", StatePath(List("approved")), DataType.Bool), OutputDecl("customer", StatePath(List("customer")), DataType.Str)),
    nodes = List(NodeDef("check", IfNode.Kind, json("""{"path":"approved"}""")))
  )

  private def trigger(id: String = "t1", queue: String = "orders", connection: String = "kafka-prod") = TriggerDef(
    id, "approve", TriggerDef.Kind.Queue, queue = Some(queue), connection = Some(connection),
    inputs = Map("approved" -> Json.fromString("$body.approved"), "customer" -> Json.fromString("$key"))
  )

  private def withApp[A](known: Ref[IO, List[Connections.Resolved]])(use: (FlowEngine[IO], TriggerRegistry[IO], FakeConsumers) => IO[A]): IO[A] =
    IOActorSystem[IO]("queue-triggers-test").use { system =>
      for {
        journal <- InMemoryJournal.make[IO]
        engine <- ActorFlowEngine.make[IO](system, journal)
        _ <- engine.store.save(flow)
        registry <- TriggerRegistry.make[IO]()
        sinks <- Ref.of[IO, Map[String, Sink[IO, Array[Byte]]]](Map.empty)
        starts <- Ref.of[IO, List[String]](Nil)
        consumers = new FakeConsumers(sinks, starts)
        connections: Connections[IO] = name => known.get.map(_.find(_.name == name))
        result <- QueueTriggers
          .resource[IO](engine, registry, connections, consumers, QueueTriggers.Settings(pollEvery = 20.millis, restartAfter = 50.millis))
          .use(_ => use(engine, registry, consumers))
          .timeout(20.seconds)
      } yield result
    }

  private def withKnown[A](use: (FlowEngine[IO], TriggerRegistry[IO], FakeConsumers) => IO[A]): IO[A] =
    Ref.of[IO, List[Connections.Resolved]](List(kafka)).flatMap(withApp(_)(use))

  private def finished(engine: FlowEngine[IO], id: String): IO[InstanceStatus] =
    engine.status(id).flatMap {
      case Some(s) if s.status != InstanceState.Status.Running => IO.pure(s)
      case _                                                   => IO.sleep(10.millis) >> finished(engine, id)
    }.timeout(5.seconds)

  "a message starts an instance of the flow with the inputs the trigger maps from it" in
    withKnown { (engine, registry, consumers) =>
      for {
        _ <- registry.put(trigger())
        _ <- consumers.push("t1", """{"approved":true}""", "0@1", key = Some("ann"))
        page <- engine.search(InstanceQuery(origin = Some("trigger:t1")))
        id = page.items.head.id
        status <- finished(engine, id)
      } yield {
        page.items should have size 1
        id shouldBe "t1-orders-0_1"
        status.state shouldBe json("""{"approved":true,"customer":"ann"}""")
        status.nodes("check").port shouldBe Some("then")
      }
    }

  "a message that is delivered again does not start a second instance" in
    withKnown { (engine, registry, consumers) =>
      for {
        _ <- registry.put(trigger())
        _ <- consumers.push("t1", """{"approved":true}""", "0@1", key = Some("ann"))
        _ <- consumers.push("t1", """{"approved":true}""", "0@1", key = Some("ann"))
        _ <- consumers.push("t1", """{"approved":false}""", "0@2", key = Some("bob"))
        page <- engine.search(InstanceQuery(origin = Some("trigger:t1")))
      } yield page.items.map(_.id).toSet shouldBe Set("t1-orders-0_1", "t1-orders-0_2")
    }

  "the id of the message in its headers, when it has one, names the instance" in
    withKnown { (engine, registry, consumers) =>
      for {
        _ <- registry.put(trigger())
        _ <- consumers.push("t1", """{"approved":true}""", "0@1", Map("x-idempotency-key" -> "inst-9:pub"), Some("ann"))
        // Another place in the broker, same message (it was published twice): one instance.
        _ <- consumers.push("t1", """{"approved":true}""", "3@7", Map("x-idempotency-key" -> "inst-9:pub"), Some("ann"))
        page <- engine.search(InstanceQuery(origin = Some("trigger:t1")))
      } yield page.items.map(_.id) shouldBe List("t1-inst-9_pub")
    }

  "a message that does not fit the flow is dropped, so it does not block the ones behind it" in
    withKnown { (engine, registry, consumers) =>
      for {
        _ <- registry.put(trigger())
        // `approved` is not a boolean, and there is no key.
        dropped <- consumers.push("t1", """{"approved":"yes"}""", "0@1").attempt
        _ <- consumers.push("t1", """{"approved":true}""", "0@2", key = Some("ann"))
        page <- engine.search(InstanceQuery(origin = Some("trigger:t1")))
      } yield {
        dropped shouldBe Right(())
        page.items.map(_.id) shouldBe List("t1-orders-0_2")
      }
    }

  "a flow that does not exist yet is a failure, so the broker delivers the message again" in
    withKnown { (_, registry, consumers) =>
      for {
        _ <- registry.put(trigger().copy(flowId = "later"))
        result <- consumers.push("t1", """{"approved":true}""", "0@1", key = Some("ann")).attempt
      } yield result.left.map(_.getClass.getSimpleName) shouldBe Left("FlowNotFound")
    }

  "triggers can change while it runs: edits restart the consumer, deletes stop it, and other kinds are ignored" in
    withKnown { (_, registry, consumers) =>
      for {
        _ <- registry.put(trigger())
        _ <- registry.put(TriggerDef("h1", "approve", TriggerDef.Kind.Http, path = Some("x")))
        _ <- consumers.push("t1", """{"approved":true}""", "0@1", key = Some("ann"))
        _ <- consumers.starts.get.map(_ shouldBe List("t1@kafka-prod"))
        _ <- registry.put(trigger(queue = "other"))
        _ <- IO.sleep(200.millis)
        _ <- consumers.starts.get.map(_ shouldBe List("t1@kafka-prod", "t1@kafka-prod"))
        _ <- consumers.running.map(_ shouldBe Set("t1"))
        _ <- registry.delete("t1")
        _ <- consumers.awaitStopped("t1")
      } yield succeed
    }

  "a trigger waits for its connection, and follows it when it changes" in
    Ref.of[IO, List[Connections.Resolved]](Nil).flatMap { known =>
      withApp(known) { (_, registry, consumers) =>
        for {
          _ <- registry.put(trigger())
          _ <- IO.sleep(150.millis)
          _ <- consumers.running.map(_ shouldBe empty)
          _ <- known.set(List(kafka))
          _ <- consumers.push("t1", """{"approved":true}""", "0@1", key = Some("ann"))
          _ <- known.set(List(kafka.copy(params = json("""{"bootstrapServers":"other:9092"}"""))))
          _ <- IO.sleep(200.millis)
          _ <- consumers.starts.get.map(_ should have size 2)
        } yield succeed
      }
    }

  "a trigger of the queue kind needs a connection, and the new fields are in its JSON" in IO {
    TriggerDef("t", "f", TriggerDef.Kind.Queue, queue = Some("q")).problems shouldBe List("a queue trigger needs a connection")
    TriggerDef("t", "f", TriggerDef.Kind.Queue, queue = Some("q"), connection = Some("c"), destinationType = Some("both")).problems should have size 1
    val t = trigger().copy(group = Some("g"), destinationType = Some("topic"))
    io.circe.Decoder[TriggerDef].decodeJson(io.circe.Encoder[TriggerDef].apply(t)) shouldBe Right(t)
  }

  "the request of a message and the id of its instance" in IO {
    val m = Incoming("orders", Some("k"), "plain text".getBytes(UTF_8), Map("h" -> "v"), "1@2")
    QueueTriggers.request(m) shouldBe json("""{"source":"orders","key":"k","position":"1@2","headers":{"h":"v"},"body":"plain text"}""")
    QueueTriggers.request(m.copy(value = Array.emptyByteArray)).hcursor.downField("body").focus shouldBe Some(Json.Null)
    QueueTriggers.instanceId(trigger(), m) shouldBe "t1-orders-1_2"
  }

  "each trigger goes to the source that reads its kind of connection, and none runs one that no source reads" in
    IOActorSystem[IO]("trigger-sources-test").use { system =>
      val mongo = Connections.Resolved("changes", "mongodb", Json.obj(), Map.empty)
      val redis = Connections.Resolved("cache", "redis", Json.obj(), Map.empty)
      val connections: Connections[IO] = name => IO.pure(List(kafka, mongo, redis).find(_.name == name))
      def awaitStarts(c: FakeConsumers, n: Int): IO[List[String]] =
        c.starts.get.flatMap(s => if (s.size >= n) IO.pure(s) else IO.sleep(10.millis) >> awaitStarts(c, n)).timeout(5.seconds)
      for {
        journal <- InMemoryJournal.make[IO]
        engine <- ActorFlowEngine.make[IO](system, journal)
        registry <- TriggerRegistry.make[IO]()
        brokers <- (Ref.of[IO, Map[String, Sink[IO, Array[Byte]]]](Map.empty), Ref.of[IO, List[String]](Nil)).mapN(new FakeConsumers(_, _))
        changes <- (Ref.of[IO, Map[String, Sink[IO, Array[Byte]]]](Map.empty), Ref.of[IO, List[String]](Nil)).mapN(new FakeConsumers(_, _))
        _ <- registry.put(trigger("t1"))
        _ <- registry.put(trigger("t2", connection = "changes"))
        _ <- registry.put(trigger("t3", connection = "cache"))
        sources = List(QueueTriggers.source[IO](brokers, _ == "kafka"), QueueTriggers.source[IO](changes, _ == "mongodb"))
        started <- TriggerSources
          .resource[IO](engine, registry, connections, sources, TriggerSources.Settings(pollEvery = 20.millis))
          .use(_ => (awaitStarts(brokers, 1), awaitStarts(changes, 1)).tupled <* IO.sleep(100.millis))
        all <- (brokers.starts.get, changes.starts.get).tupled
      } yield {
        started shouldBe ((List("t1@kafka-prod"), List("t2@changes")))
        all shouldBe ((List("t1@kafka-prod"), List("t2@changes")))
      }
    }
}
