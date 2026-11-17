package xplat.flow.plugin.testkit

import cats.effect.{IO, Resource}
import io.circe.Json
import xplat.flow.model.NodeInput
import xplat.flow.nodes.Connections

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.io.Source

/** The Python example (`modules/flow/plugin-example-python`, with the SDK copied in) passes the same contract as a jvm plugin. */
class PythonExampleContract extends PluginContract {

  private val flowDir: Path = {
    val s = Source.fromResource("flow-dir.path")
    try Paths.get(s.mkString.trim)
    finally s.close()
  }

  /** The directory as it would be installed: the example and the SDK. */
  private lazy val installed: Path = {
    val d = Files.createDirectories(Files.createTempDirectory("py-plugin").resolve("pyexample"))
    List(flowDir.resolve("plugin-example-python/flow-plugin.json"), flowDir.resolve("plugin-example-python/main.py"), flowDir.resolve("plugin-sdk/python/xflow_plugin.py"))
      .foreach(f => Files.copy(f, d.resolve(f.getFileName), StandardCopyOption.REPLACE_EXISTING))
    d
  }

  def plugin: Resource[IO, PluginHarness] =
    PluginHarness.directory(installed, PluginHarness.connections(Connections.Resolved("signing", "pyexample", Json.obj(), Map("key" -> "k"))))

  override def samples: Map[String, List[NodeInput]] = Map(
    "count-words" -> List(NodeInput(Json.obj("text" -> Json.fromString("${state.t}")), Json.obj("t" -> Json.fromString("a b c")))),
    "sign" -> List(NodeInput(Json.obj("connection" -> Json.fromString("signing"), "text" -> Json.fromString("hola")), Json.obj()))
  )
}
