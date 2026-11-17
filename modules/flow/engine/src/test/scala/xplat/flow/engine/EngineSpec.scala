package xplat.flow.engine

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine.InstanceState.NodeStatus
import xplat.flow.model._

import scala.concurrent.duration._

/** What the tests of the engine share: building flows, engines on an in-memory journal, and waiting for instances. */
abstract class EngineSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  protected def json(text: String): Json = parse(text).fold(e => throw e, j => j)
  protected def path(text: String): StatePath = StatePath(text.split('.').toList)
  protected def out(name: String): OutputDecl = OutputDecl("v", path(s"out.$name"), DataType.AnyJson)

  protected def step(id: String, extra: (String, Json)*): NodeDef =
    NodeDef(id, "test.step", Json.obj(("tag" -> Json.fromString(id)) +: extra: _*), List(out(id)))

  protected def flow(id: String, nodes: List[NodeDef], edges: List[Edge], inputs: List[OutputDecl] = Nil, outputs: List[OutputDecl] = Nil): FlowDef =
    FlowDef(id, id, nodes, edges, inputs, outputs)

  protected def withEngine[A](f: (FlowEngine[IO], Probe) => IO[A]): IO[A] = withEngineConfig(ActorFlowEngine.Config())(f)

  protected def withEngineConfig[A](config: ActorFlowEngine.Config)(f: (FlowEngine[IO], Probe) => IO[A]): IO[A] =
    for {
      journal <- InMemoryJournal.make[IO]
      probe <- Probe.make
      result <- IOActorSystem[IO]("flows").use(system => ActorFlowEngine.make[IO](system, journal, List(probe.step, probe.write, probe.secret), config).flatMap(f(_, probe)))
    } yield result

  protected def save(engine: FlowEngine[IO], flow: FlowDef): IO[FlowDef] =
    engine.store.save(flow).flatMap(r => IO.fromEither(r.left.map(e => new RuntimeException(e.map(_.message).mkString("; ")))))

  protected def run(engine: FlowEngine[IO], flow: FlowDef, inputs: Json = Json.obj()): IO[InstanceId] =
    save(engine, flow).flatMap(f => engine.start(f.id, None, inputs, None))

  protected def finished(engine: FlowEngine[IO], id: InstanceId): IO[InstanceStatus] =
    engine.events(id).compile.drain >> engine.status(id).map(_.get)

  protected def awaitUntil[A](io: IO[A])(p: A => Boolean): IO[A] =
    io.flatMap(a => if (p(a)) IO.pure(a) else IO.sleep(20.millis) >> awaitUntil(io)(p))

  protected def valueAt(s: InstanceStatus, p: String): Option[Json] = path(p).get(s.state)

  protected def status(s: InstanceStatus, node: String): NodeStatus = s.nodes(node).status

  protected def edge(from: String, to: String, port: String = Port.Out) = Edge(from, to, port)


  /** Where the first entry of that kind about the node is in the trail. */
  protected def position(entries: List[AuditEntry], kind: String, node: String): Int =
    entries.indexWhere(e => e.kind == kind && e.nodeOf.contains(node))

  protected val IfNodeKind = "core.if"
}
