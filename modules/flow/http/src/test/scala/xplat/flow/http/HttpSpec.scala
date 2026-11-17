package xplat.flow.http

import xplat.flow.nodes.ConnectionKind
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{EntityEncoder, Header, Method, Request, Status, Uri}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString
import xplat.core.io.actor.IOActorSystem
import xplat.flow.engine.{ActorFlowEngine, Deployments, FlowEngine, InMemoryJournal}
import xplat.flow.model._
import xplat.flow.nodes.{IfNode, NodeType, RespondNode}

import scala.concurrent.duration._

/** The routes under test, over a real engine with the actors in memory, and requests run without a network. */
abstract class HttpSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  final case class Response(status: Status, text: String, headers: Map[String, String]) {
    def json: Json = parse(text).fold(e => throw new RuntimeException(s"not JSON: $text", e), identity)
  }

  final class App(
      val engine: FlowEngine[IO],
      val triggers: TriggerRegistry[IO],
      val connections: ConnectionStore[IO],
      val secrets: SecretStore[IO],
      val deployments: Deployments[IO],
      val drafts: FlowDrafts[IO],
      entities: Option[EntityStore[IO]] = None
  ) {
    private val routes = FlowHttp.routes[IO](engine, triggers, connections, secrets, deployments, drafts, entities = entities).orNotFound

    def call(method: Method, uri: String, body: Option[Json] = None, headers: List[(String, String)] = Nil, rawBody: Option[String] = None): IO[Response] = {
      val base = Request[IO](method, Uri.unsafeFromString(uri)).putHeaders(headers.map { case (k, v) => Header.Raw(CIString(k), v) })
      val req = (body, rawBody) match {
        case (Some(b), _)    => base.withEntity(b)
        case (_, Some(text)) => base.withEntity(text)(EntityEncoder.stringEncoder[IO])
        case _               => base
      }
      routes.run(req).flatMap(r => r.bodyText.compile.string.map(t => Response(r.status, t, r.headers.headers.map(h => h.name.toString.toLowerCase -> h.value).toMap)))
    }

    def get(uri: String): IO[Response] = call(Method.GET, uri)
    def put(uri: String, body: Json): IO[Response] = call(Method.PUT, uri, Some(body))
    def post(uri: String, body: Json, headers: List[(String, String)] = Nil): IO[Response] = call(Method.POST, uri, Some(body), headers)
    def delete(uri: String): IO[Response] = call(Method.DELETE, uri)

    def save(flow: FlowDef): IO[Response] = put(s"/api/flows/${flow.id}", flow.asJson)

    /** Waits, up to a few seconds, until the instance has ended. */
    def finished(id: String): IO[Json] = poll(id).timeout(5.seconds)

    private def poll(id: String): IO[Json] =
      get(s"/api/instances/$id").flatMap { r =>
        val status = r.json.hcursor.get[String]("status").toOption
        if (r.status == Status.Ok && status.exists(_ != "running")) IO.pure(r.json) else IO.sleep(20.millis) >> poll(id)
      }
  }

  /** A node that has a secret in its configuration, for the tests of secrets. */
  object SecretNode extends NodeType[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor(
      kind = "test.secret",
      category = "Test",
      label = "Secret",
      config = ConfigSchema(List(ConfigSchema.Field("apiKey", DataType.Str, secret = true), ConfigSchema.Field("note", DataType.Str, required = false)))
    )
    def run(input: NodeInput): IO[NodeOutput] = IO.pure(NodeOutput.ok())
  }

  def withApp[A](f: App => IO[A]): IO[A] =
    IOActorSystem[IO]("http-test").use { system =>
      for {
        journal <- InMemoryJournal.make[IO]
        engine <- ActorFlowEngine.make[IO](system, journal, extraNodes = List(SecretNode))
        triggers <- TriggerRegistry.make[IO]()
        secrets <- SecretStore.make[IO]()
        connections <- ConnectionStore.make[IO](ConnectionKind.defaults, secrets)
        deployments <- Deployments.inMemory[IO]
        drafts <- FlowDrafts.inMemory[IO]
        result <- f(new App(engine, triggers, connections, secrets, deployments, drafts)).timeout(20.seconds)
      } yield result
    }

  def path(text: String): StatePath = StatePath.parse(text).toOption.get
  def json(text: String): Json = parse(text).fold(e => throw e, identity)

  /** check (If on `approved`) -then-> ok (respond 201 with `order`) ; -else-> no (respond 409 with no body). */
  val approval: FlowDef = FlowDef(
    "approval",
    "Approval",
    inputs = List(OutputDecl("approved", path("approved"), DataType.Bool), OutputDecl("order", path("order"), DataType.AnyJson)),
    nodes = List(
      NodeDef("check", IfNode.Kind, json("""{"path":"approved"}""")),
      NodeDef("ok", RespondNode.Kind, json("""{"status":201,"body":"order","headers":{"X-Decision":"yes"}}""")),
      NodeDef("no", RespondNode.Kind, json("""{"status":409}"""))
    ),
    edges = List(Edge("check", "ok", IfNode.Then), Edge("check", "no", IfNode.Else))
  )

  def trigger(extra: TriggerDef => TriggerDef = identity): TriggerDef =
    extra(
      TriggerDef(
        "orders",
        "approval",
        TriggerDef.Kind.Http,
        method = Some("POST"),
        path = Some("orders"),
        inputs = Map("approved" -> Json.fromString("$body.approved"), "order" -> Json.fromString("$body.order"))
      )
    )
}
