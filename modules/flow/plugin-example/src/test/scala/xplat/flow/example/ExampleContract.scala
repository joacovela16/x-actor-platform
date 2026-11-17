package xplat.flow.example

import cats.effect.{IO, Resource}
import io.circe.Json
import xplat.flow.model.NodeInput
import xplat.flow.plugin.testkit.{PluginContract, PluginHarness}

import java.nio.file.{Path, Paths}
import scala.io.Source

/** The example passes the contract, loaded from its jar as the server would. */
class ExampleContract extends PluginContract {

  private def path(resource: String): Path = {
    val s = Source.fromResource(resource)
    try Paths.get(s.mkString.trim)
    finally s.close()
  }

  def plugin: Resource[IO, PluginHarness] = PluginHarness.jvm(path("plugin.jar.path"), path("plugin.manifest.path"))

  override def samples: Map[String, List[NodeInput]] = Map(
    "greet" -> List(NodeInput(Json.obj("name" -> Json.fromString("Ana")), Json.obj())),
    // The reference is resolved before the node sees it.
    "shout" -> List(NodeInput(Json.obj("text" -> Json.fromString("${state.who}")), Json.obj("who" -> Json.fromString("ana"))))
  )

  "greet writes the greeting" in {
    import cats.effect.unsafe.implicits.global
    plugin.use(h => IO(h.run("greet", Json.obj("name" -> Json.fromString("Ana"))).values("greeting"))).unsafeRunSync() shouldBe Json.fromString("Hello, Ana")
  }
}
