package xplat.flow.plugin

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.kernel.{Async, Resource}
import xplat.flow.model.{NodeDescriptor, NodeInput, NodeOutput, Port}
import xplat.flow.nodes.{ClientPool, ConnectionKind, Connections, NodeType}
import xplat.flow.plugin.PluginStatus.State

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.io.Source

/** Loads the example plugin (`modules/flow/plugin-example`) from its jar, as the server does: it is not on the classpath of
  * these tests, so whatever is found comes through the plugin's own class loader.
  */
class PluginLoaderTest extends AnyFreeSpec with Matchers {

  private val exampleJar: Path = {
    val s = Source.fromResource("example-plugin.path")
    try Paths.get(s.mkString.trim)
    finally s.close()
  }

  /** `modules/flow`, where the Python example and its SDK are. */
  private val flowDir: Path = {
    val s = Source.fromResource("python-plugin.path")
    try Paths.get(s.mkString.trim)
    finally s.close()
  }

  /** The Python example, as it would be installed: its files and the SDK in one directory. */
  private def installPython(root: Path, dir: String): Unit = {
    val d = Files.createDirectories(root.resolve(dir))
    List(flowDir.resolve("plugin-example-python/flow-plugin.json"), flowDir.resolve("plugin-example-python/main.py"), flowDir.resolve("plugin-sdk/python/xflow_plugin.py"))
      .foreach(f => Files.copy(f, d.resolve(f.getFileName), StandardCopyOption.REPLACE_EXISTING))
  }

  private def manifest(name: String = "example", version: String = "1.0.0", api: String = FlowPlugin.ApiVersion, transport: String = "jvm"): String =
    s"""{"name":"$name","version":"$version","apiVersion":"$api","transport":"$transport","command":["python3","main.py"]}"""

  /** A plugin directory under `root`: the manifest and, unless `jar` is false, the example's jar. */
  private def install(root: Path, dir: String, manifestJson: String, jar: Boolean = true): Unit = {
    val d = Files.createDirectories(root.resolve(dir))
    Files.writeString(d.resolve(PluginManifest.FileName), manifestJson)
    if (jar) Files.copy(exampleJar, d.resolve("example.jar"), StandardCopyOption.REPLACE_EXISTING)
  }

  private def withPlugins[A](
      setup: Path => Unit,
      strict: Boolean = false,
      builtIn: Set[String] = Set("http"),
      connections: Connections[IO] = Connections.none[IO](IO.pure),
      builtins: List[PluginLoader.Builtin] = Nil
  )(f: PluginLoader.Loaded[IO] => A): A = {
    val root = Files.createTempDirectory("flow-plugins")
    setup(root)
    val context = FlowPlugin.Context.basic[IO](connections, null.asInstanceOf[ClientPool[IO]])
    PluginLoader.load[IO](PluginLoader.Settings(root, strict), context, builtIn, line => IO(System.err.println(line)), builtins).use(l => IO(f(l))).unsafeRunSync()
  }

  /** A plugin of the server with nodes of the given kinds and kinds of connection, that counts how many times it was released. */
  private final class Fake(kinds: List[String], conns: List[String] = Nil, released: java.util.concurrent.atomic.AtomicInteger = new java.util.concurrent.atomic.AtomicInteger)
      extends FlowPlugin {
    override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
      Resource.make(Async[F].pure(FlowPlugin.Installed(kinds.map(node[F]))))(_ => Async[F].map(Async[F].delay(released.incrementAndGet()))(_ => ()))
    override def connectionKinds: List[ConnectionKind] = conns.map(k => ConnectionKind(k, k, xplat.flow.model.ConfigSchema(Nil)))
  }

  private def node[F[_]: Async](k: String): NodeType[F] = new NodeType[F] {
    val descriptor: NodeDescriptor = NodeDescriptor(kind = k, category = "Test", label = k)
    def run(input: NodeInput): F[NodeOutput] = Async[F].pure(NodeOutput.ok(Map.empty))
  }

  private def failure(l: PluginLoader.Loaded[IO], dir: String): String =
    l.statuses.find(_.dir == dir).map(_.state) match {
      case Some(State.Failed(reason)) => reason
      case other                      => fail(s"$dir did not fail: $other")
    }

  "the example plugin is not on the test classpath" in {
    an[ClassNotFoundException] should be thrownBy Class.forName("xplat.flow.example.ExamplePlugin")
  }

  "loads the nodes of a jar under <plugin>.<kind>@<major>, written in Scala and in Java" in {
    withPlugins(root => install(root, "example", manifest())) { l =>
      l.failed shouldBe Nil
      l.nodes.map(_.descriptor.kind) should contain theSameElementsAs List("example.greet@1", "example.shout@1")
      l.connectionKinds.map(_.kind) shouldBe List("example")

      val greet = l.nodes.find(_.descriptor.kind == "example.greet@1").get
      val out = greet.run(NodeInput(Json.obj("name" -> Json.fromString("Ana")), Json.obj())).unsafeRunSync()
      out.values shouldBe Map("greeting" -> Json.fromString("Hello, Ana"))

      val shout = l.nodes.find(_.descriptor.kind == "example.shout@1").get
      shout.run(NodeInput(Json.obj("text" -> Json.fromString("hola")), Json.obj())).unsafeRunSync().values shouldBe
        Map("shouted" -> Json.fromString("HOLA"))
      shout.run(NodeInput(Json.obj(), Json.obj())).unsafeRunSync().port shouldBe Port.Error
    }
  }

  "the params reach the node with their references resolved, and the validator sees what they read" in {
    withPlugins(root => install(root, "example", manifest())) { l =>
      val shout = l.nodes.find(_.descriptor.kind == "example.shout@1").get
      val params = Json.obj("text" -> Json.fromString("${state.greeting} (${context.nodeId})"))
      val state = Json.obj("greeting" -> Json.fromString("Hello, Ana"))
      shout.run(NodeInput(params, state, nodeId = "shout")).unsafeRunSync().values shouldBe
        Map("shouted" -> Json.fromString("HELLO, ANA (SHOUT)"))

      // A reference to something that is not in the state fails the node, without running the plugin's code.
      val missing = shout.run(NodeInput(params, Json.obj())).unsafeRunSync()
      missing.port shouldBe Port.Error
      missing.error.get should include("greeting")

      shout.descriptor.reads(params).map(_.path.render) shouldBe List("greeting")
      shout.descriptor.checkParams(Json.obj("text" -> Json.fromString("${nope.x}"))) should not be empty
    }
  }

  "two majors of a plugin live side by side; the newest keeps the kind of connection" in {
    withPlugins { root =>
      install(root, "example-1", manifest(version = "1.2.0"))
      install(root, "example-2", manifest(version = "2.0.0"))
    } { l =>
      l.failed shouldBe Nil
      l.nodes.map(_.descriptor.kind).toSet shouldBe Set("example.greet@1", "example.shout@1", "example.greet@2", "example.shout@2")
      l.connectionKinds.map(_.kind) shouldBe List("example")
    }
  }

  "the same major installed twice: the second one fails" in {
    withPlugins { root =>
      install(root, "a", manifest(version = "1.0.0"))
      install(root, "b", manifest(version = "1.1.0"))
    } { l =>
      l.nodes.map(_.descriptor.kind) should contain theSameElementsAs List("example.greet@1", "example.shout@1")
      failure(l, "b") should include("installed twice")
    }
  }

  "plugins that cannot be loaded are reported and left out" in {
    withPlugins { root =>
      install(root, "api", manifest(name = "old", api = "0.9.0"))
      install(root, "process", manifest(name = "py", transport = "process"), jar = false)  // python3 main.py: there is no main.py
      install(root, "broken", "{ not json", jar = false)
      install(root, "nojar", manifest(name = "nojar"), jar = false)
      install(root, "ok", manifest())
    } { l =>
      failure(l, "api") should include("plugin API 0.9.0")
      failure(l, "process") should include("ended")
      failure(l, "broken") should include(PluginManifest.FileName)
      failure(l, "nojar") should include("no jar")
      l.nodes.map(_.descriptor.kind) should contain theSameElementsAs List("example.greet@1", "example.shout@1")
    }
  }

  "a plugin cannot declare a kind of connection of the server or of another plugin" in {
    withPlugins(root => install(root, "example", manifest()), builtIn = Set("example")) { l =>
      failure(l, "example") should include("kinds of connection of the server")
      l.nodes shouldBe Nil
    }
    // Installed under another name, its kind of connection `example` is not its own.
    withPlugins(root => install(root, "other", manifest(name = "other"))) { l =>
      failure(l, "other") should include("must be 'other' or start with 'other.'")
    }
  }

  "strict: a plugin that fails stops the start" in {
    val e = the[IllegalStateException] thrownBy withPlugins(root => install(root, "nojar", manifest(), jar = false), strict = true)(identity)
    e.getMessage should include("example@1: no jar")
  }

  "no plugins directory: nothing to load" in {
    val context = FlowPlugin.Context.basic[IO](Connections.none[IO](IO.pure), null.asInstanceOf[ClientPool[IO]])
    val loaded = PluginLoader.load[IO](PluginLoader.Settings(Paths.get("/does/not/exist")), context, Set.empty, _ => IO.unit).use(IO.pure).unsafeRunSync()
    loaded.nodes shouldBe Nil
    loaded.statuses shouldBe Nil
  }

  "process: a plugin in Python, through the SDK, with its params resolved and its own connections" in {
    val key = Connections.Resolved("signing", "pyexample", Json.obj(), Map("key" -> "k"))
    val http = Connections.Resolved("api", "http", Json.obj(), Map.empty)
    val connections: Connections[IO] = (name: String) => IO.pure(List(key, http).find(_.name == name))
    withPlugins(root => installPython(root, "pyexample"), connections = connections) { l =>
      l.failed shouldBe Nil
      l.nodes.map(_.descriptor.kind) should contain theSameElementsAs List("pyexample.count-words@1", "pyexample.sign@1")
      l.connectionKinds.map(_.kind) shouldBe List("pyexample")

      val count = l.nodes.find(_.descriptor.kind == "pyexample.count-words@1").get
      count.descriptor.defaultOutputs(Json.obj()).map(_.name) shouldBe List("words")
      val state = Json.obj("title" -> Json.fromString("uno dos tres"))
      count.run(NodeInput(Json.obj("text" -> Json.fromString("${state.title} cuatro")), state)).unsafeRunSync().values shouldBe
        Map("words" -> Json.fromInt(4))

      // Several at once: the answers find their requests.
      val many = (1 to 20).toList.map(n => count.run(NodeInput(Json.obj("text" -> Json.fromString(List.fill(n)("w").mkString(" "))), Json.obj())))
      import cats.syntax.all._
      many.parSequence.unsafeRunSync().map(_.values("words")) shouldBe (1 to 20).map(Json.fromInt).toList

      val sign = l.nodes.find(_.descriptor.kind == "pyexample.sign@1").get
      val signed = sign.run(NodeInput(Json.obj("connection" -> Json.fromString("signing"), "text" -> Json.fromString("hola")), Json.obj())).unsafeRunSync()
      signed.values("signature").asString.get should have length 64
      // A connection of a kind that is not the plugin's never reaches it.
      sign.run(NodeInput(Json.obj("connection" -> Json.fromString("api"), "text" -> Json.fromString("x")), Json.obj())).unsafeRunSync().error.get should
        include("not one of the plugin pyexample's")
      // A node that throws fails with the reason.
      sign.run(NodeInput(Json.obj("connection" -> Json.fromString("signing")), Json.obj())).unsafeRunSync().error.get should include("KeyError")
    }
  }

  "built-in plugins: their kinds keep their names, they come first and are released when the server stops" in {
    val released = new java.util.concurrent.atomic.AtomicInteger
    withPlugins(
      root => install(root, "example", manifest()),
      builtins = List(PluginLoader.Builtin("db", new Fake(List("db.query"), List("postgres"), released)), PluginLoader.Builtin("core", new Fake(List("core.if"))))
    ) { l =>
      l.failed shouldBe Nil
      l.nodes.map(_.descriptor.kind) shouldBe List("db.query", "core.if", "example.greet@1", "example.shout@1")
      l.connectionKinds.map(_.kind) shouldBe List("postgres", "example")
      l.statuses.map(s => s.dir -> s.builtin) shouldBe List("db" -> true, "core" -> true, "example" -> false)
      io.circe.Encoder[PluginStatus].apply(l.statuses.head).hcursor.get[String]("source") shouldBe Right("builtin")
      released.get shouldBe 0
    }
    released.get shouldBe 1
  }

  "built-in plugins: a kind outside the plugin's name or a kind of connection taken stops the start" in {
    val foreign = the[Exception] thrownBy withPlugins(_ => (), builtins = List(PluginLoader.Builtin("db", new Fake(List("mongo.find")))))(identity)
    foreign.getMessage should include("built-in plugin db: kinds must start with 'db.': mongo.find")
    val taken = the[Exception] thrownBy withPlugins(_ => (), builtins = List(PluginLoader.Builtin("db", new Fake(List("db.query"), List("http")))))(identity)
    taken.getMessage should include("kinds of connection that already exist: http")
  }

  "a plugin of the directory cannot take the name of a built-in one" in {
    withPlugins(root => install(root, "example", manifest()), builtins = List(PluginLoader.Builtin("example", new Fake(List("example.x"))))) { l =>
      l.failed.map(_.state) shouldBe List(State.Failed("'example' is the name of a plugin of the server"))
      l.nodes.map(_.descriptor.kind) shouldBe List("example.x")
    }
  }
}
