package xplat.flow.plugin

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all._
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.{ConnectionKind, Connections, NodeType}

import java.util.concurrent.CompletableFuture
import java.util.Optional
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

/** A plugin written in Java (or Kotlin), without `F[_]`: its nodes answer a `CompletableFuture`.
  *
  * {{{
  * public final class IpaPlugin extends JavaFlowPlugin {
  *   public List<JavaFlowPlugin.Node> javaNodes(JavaFlowPlugin.Context context) { return List.of(new Lookup(context)); }
  * }
  * }}}
  */
abstract class JavaFlowPlugin extends FlowPlugin {

  def javaNodes(context: JavaFlowPlugin.Context): java.util.List[JavaFlowPlugin.Node]

  def javaConnectionKinds(): java.util.List[ConnectionKind] = java.util.Collections.emptyList()

  final override def connectionKinds: List[ConnectionKind] = javaConnectionKinds().asScala.toList

  final override def nodes[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, List[NodeType[F]]] =
    Dispatcher.parallel[F](await = true).evalMap { dispatcher =>
      val javaContext = new JavaFlowPlugin.Context {
        def connection(name: String): CompletableFuture[Optional[Connections.Resolved]] =
          dispatcher.unsafeToCompletableFuture(context.connections.resolve(name).map(_.toJava))
      }
      Async[F].delay(javaNodes(javaContext).asScala.toList.map(JavaFlowPlugin.adapt[F]))
    }
}

object JavaFlowPlugin {

  /** A kind of node. `run` must not block the calling thread: do slow work in the future it returns. */
  trait Node {
    def descriptor(): NodeDescriptor
    def run(input: NodeInput): CompletableFuture[NodeOutput]

    /** What it would answer in a simulation, without touching anything outside. Empty: the engine makes it up. */
    def simulated(input: NodeInput): Optional[NodeOutput] = Optional.empty()
  }

  trait Context {

    /** The named connection, with its secrets resolved; empty if it does not exist. */
    def connection(name: String): CompletableFuture[Optional[Connections.Resolved]]
  }

  /** A Java node as a [[NodeType]]. */
  def adapt[F[_]: Async](node: Node): NodeType[F] = {
    val d = node.descriptor()
    new NodeType[F] {
      val descriptor: NodeDescriptor = d
      def run(input: NodeInput): F[NodeOutput] = Async[F].fromFuture(Async[F].delay(node.run(input).asScala))
      override def simulated(input: NodeInput): Option[NodeOutput] = node.simulated(input).toScala
    }
  }

  // ---- helpers, because Java cannot use the default arguments of the Scala types ----

  /** A field of the configuration. `required` fields without a value make the flow invalid. */
  def field(name: String, tpe: DataType, required: Boolean, description: String): ConfigSchema.Field =
    ConfigSchema.Field(name, tpe, required = required, description = Option(description))

  /** A node with one `out` port that writes `outputs` (each to the state path of the same name). */
  def descriptor(
      kind: String,
      category: String,
      label: String,
      sideEffects: SideEffects,
      fields: java.util.List[ConfigSchema.Field],
      outputs: java.util.Map[String, DataType]
  ): NodeDescriptor = {
    val decls = outputs.asScala.toList.map { case (name, tpe) => OutputDecl(name, StatePath(name.split('.').toList), tpe) }
    NodeDescriptor(kind, category, label, ConfigSchema(fields.asScala.toList), sideEffects, defaultOutputs = _ => decls)
  }

  /** A text parameter of the node, if it is set. */
  def text(input: NodeInput, name: String): Optional[String] = input.params.hcursor.get[String](name).toOption.filter(_.nonEmpty).toJava

  /** Finishes through `out` writing these values. */
  def ok(values: java.util.Map[String, Json]): NodeOutput = NodeOutput.ok(values.asScala.toMap)

  def failed(reason: String): NodeOutput = NodeOutput.failed(reason)

  val Pure: SideEffects = SideEffects.Pure
  val Read: SideEffects = SideEffects.Read
  val Write: SideEffects = SideEffects.Write
  val Str: DataType = DataType.Str
  val Num: DataType = DataType.Num
  val Bool: DataType = DataType.Bool
  val AnyJson: DataType = DataType.AnyJson

  def string(s: String): Json = Json.fromString(s)
}
