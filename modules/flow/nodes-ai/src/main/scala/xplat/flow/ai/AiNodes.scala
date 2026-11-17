package xplat.flow.ai

import xplat.flow.nodes.ConnectionKind
import cats.effect.{Async, Resource}
import org.http4s.ember.client.EmberClientBuilder
import xplat.flow.model.NodeCatalog
import xplat.flow.nodes.{Connections, NodeType}

/** The AI nodes, to give to the engine as `extraNodes`.
  *
  * {{{
  * AiNodes.resource[F](connections).use(nodes => ActorFlowEngine.make[F](system, journal, extraNodes = nodes))
  * }}}
  *
  * The HTTP client that they use is opened with the resource and closed with it. `cassettes` records the answers of the service to files
  * or plays them back, to run flows that use an agent without calling it.
  */
object AiNodes {

  /** The kinds of connection that these nodes need, to pass to the `ConnectionStore`. */
  val connectionKinds: List[ConnectionKind] = AiKinds.all

  /** What the nodes are, for validation and the editor, without anything to run. */
  val catalog: NodeCatalog = new NodeCatalog(List(AiNode.descriptor))

  def nodes[F[_]](connections: Connections[F], transport: AiTransport[F], settings: AiNode.Settings = AiNode.Settings())(implicit A: Async[F]): List[NodeType[F]] =
    List(new AiNode[F](connections, transport, settings))

  def resource[F[_]](connections: Connections[F], cassettes: Cassettes.Mode = Cassettes.Mode.Off, settings: AiNode.Settings = AiNode.Settings())(implicit A: Async[F]): Resource[F, List[NodeType[F]]] =
    EmberClientBuilder.default[F].build.map(client => nodes[F](connections, Cassettes[F](AiTransport.http(client), cassettes), settings))
}
