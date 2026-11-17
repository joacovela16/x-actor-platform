package xplat.flow.plugin.testkit

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import io.circe.Json
import xplat.flow.model.{NodeInput, NodeOutput}
import xplat.flow.nodes.{ClientPool, Connections, NodeType}
import xplat.flow.plugin.{FlowPlugin, PluginLoader, PluginManifest, PluginStatus}

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.jdk.StreamConverters._

/** A plugin loaded as the server loads it: from its directory (manifest, jars or the files of its process), in a class loader of its
  * own or a process, with its kinds named `<plugin>.<kind>@<major>` and the references of the params resolved before its nodes run.
  *
  * {{{
  * PluginHarness.jvm(jar, manifest).use { h =>
  *   IO(h.run("greet", Json.obj("name" -> "Ana".asJson)).values shouldBe Map("greeting" -> "Hello, Ana".asJson))
  * }
  * }}}
  */
final class PluginHarness private (val loaded: PluginLoader.Loaded[IO], val manifest: PluginManifest) {

  def statuses: List[PluginStatus] = loaded.statuses

  def nodes: List[NodeType[IO]] = loaded.nodes

  /** A node by its full kind (`example.greet@1`) or by the one the plugin gave it (`greet`). */
  def node(kind: String): NodeType[IO] =
    nodes
      .find(n => n.descriptor.kind == kind || n.descriptor.kind == manifest.kindName(kind))
      .getOrElse(throw new NoSuchElementException(s"no node '$kind'; there are ${nodes.map(_.descriptor.kind).mkString(", ")}"))

  /** Runs a node with these params and state, as the engine would (blocking: for tests). */
  def run(kind: String, params: Json, state: Json = Json.obj()): NodeOutput = node(kind).run(NodeInput(params, state, nodeId = "test")).unsafeRunSync()

  def run(kind: String, input: NodeInput): NodeOutput = node(kind).run(input).unsafeRunSync()
}

object PluginHarness {

  /** The connections of a test: the ones given, by name. */
  def connections(cs: Connections.Resolved*): Connections[IO] = (name: String) => IO.pure(cs.find(_.name == name))

  /** Loads the plugin whose directory is `dir` (with its `flow-plugin.json`). It fails if the server would not load it. */
  def directory(dir: Path, connections: Connections[IO] = PluginHarness.connections()): Resource[IO, PluginHarness] =
    for {
      root <- Resource.make(IO.blocking(Files.createTempDirectory("flow-plugin-test")))(r => IO.blocking(delete(r)))
      _ <- Resource.eval(IO.blocking(copy(dir, root.resolve(dir.getFileName.toString))))
      h <- load(root, connections)
    } yield h

  /** Loads a `jvm` plugin from its jar and its manifest (a file, or its JSON). */
  def jvm(jar: Path, manifest: String, connections: Connections[IO]): Resource[IO, PluginHarness] =
    for {
      root <- Resource.make(IO.blocking(Files.createTempDirectory("flow-plugin-test")))(r => IO.blocking(delete(r)))
      _ <- Resource.eval(IO.blocking {
        val d = Files.createDirectories(root.resolve("plugin"))
        Files.writeString(d.resolve(PluginManifest.FileName), manifest)
        Files.copy(jar, d.resolve(jar.getFileName), StandardCopyOption.REPLACE_EXISTING)
      })
      h <- load(root, connections)
    } yield h

  def jvm(jar: Path, manifest: Path): Resource[IO, PluginHarness] = jvm(jar, Files.readString(manifest), connections())

  private def load(root: Path, connections: Connections[IO]): Resource[IO, PluginHarness] =
    PluginLoader
      .load[IO](PluginLoader.Settings(root, strict = true), FlowPlugin.Context.basic[IO](connections, null.asInstanceOf[ClientPool[IO]]), Set("secret", "http"), line => IO(System.err.println(line)))
      .evalMap { l =>
        l.statuses.flatMap(_.manifest).headOption match {
          case Some(m) => IO.pure(new PluginHarness(l, m))
          case None    => IO.raiseError(new IllegalStateException("no plugin found"))
        }
      }

  private def copy(from: Path, to: Path): Unit = {
    val s = Files.walk(from)
    try s.toScala(List).foreach { p =>
      val target = to.resolve(from.relativize(p).toString)
      if (Files.isDirectory(p)) Files.createDirectories(target) else Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING)
    }
    finally s.close()
  }

  private def delete(dir: Path): Unit = {
    val s = Files.walk(dir)
    try s.toScala(List).reverse.foreach(Files.deleteIfExists)
    finally s.close()
  }
}
