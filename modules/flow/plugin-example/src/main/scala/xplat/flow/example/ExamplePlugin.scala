package xplat.flow.example

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.{ConnectionKind, NodeType}
import xplat.flow.plugin.{FlowPlugin, JavaFlowPlugin}

/** The example plugin: `greet` in Scala and `shout` in Java (see [[ShoutNode]]), plus a kind of connection. Installed as
  * `example`, its nodes are `example.greet@1` and `example.shout@1`.
  */
final class ExamplePlugin extends FlowPlugin {

  override def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    Resource.pure(FlowPlugin.Installed(List(new ExamplePlugin.Greet[F], ExamplePlugin.shout[F])))

  override def connectionKinds: List[ConnectionKind] =
    List(ConnectionKind("example", "Example service", ConfigSchema(List(ConfigSchema.Field("apiKey", DataType.Str, secret = true)))))
}

object ExamplePlugin {

  /** Writes `greeting`: "Hello, <name>". */
  final class Greet[F[_]: Async] extends NodeType[F] {

    val descriptor: NodeDescriptor = NodeDescriptor(
      kind = "greet",
      category = "Example",
      label = "Greet",
      description = "Example node: greets a name.",
      config = ConfigSchema(List(ConfigSchema.Field("name", DataType.Str, description = Some("Who to greet")))),
      defaultOutputs = _ => List(OutputDecl("greeting", StatePath(List("greeting")), DataType.Str))
    )

    def run(input: NodeInput): F[NodeOutput] =
      input.params.hcursor.get[String]("name").toOption.filter(_.nonEmpty) match {
        case Some(name) => NodeOutput.ok(Map("greeting" -> Json.fromString(s"Hello, $name"))).pure[F]
        case None       => NodeOutput.failed("name is empty").pure[F]
      }
  }

  /** The Java node goes through the same adapter as a Java plugin would. */
  private def shout[F[_]: Async]: NodeType[F] = JavaFlowPlugin.adapt[F](new ShoutNode)
}
