package xplat.flow.script

import cats.effect.Async
import xplat.flow.model.NodeCatalog
import xplat.flow.nodes.NodeType
import xplat.flow.stream.{StreamNodeType, StreamPlugin}

/** The nodes of the module, to give to the engine and to `StreamNodes`.
  *
  * {{{
  * val scripts = ScriptNodes.nodes[F]()                 // extraNodes of the engine
  * StreamNodes.resource[F](connections, metrics, extra = ScriptNodes.streamNodes[F]())
  * }}}
  */
object ScriptNodes {

  /** What the nodes are, for validation and the editor, without anything to run. */
  val catalog: NodeCatalog = new NodeCatalog(List(ScriptNode.descriptor, StreamScript.descriptor))

  def nodes[F[_]](limits: ScriptSandbox.Limits = ScriptSandbox.Limits())(implicit F: Async[F]): List[NodeType[F]] =
    List(new ScriptNode[F](limits))

  def streamNodes[F[_]](limits: ScriptSandbox.Limits = ScriptSandbox.Limits())(implicit F: Async[F]): List[StreamNodeType[F]] =
    List(new StreamScript[F](limits))

  /** `stream.script`, for the plugin `stream`. */
  val streamExtra: StreamPlugin.Extra = new StreamPlugin.Extra {
    def apply[F[_]: Async]: List[StreamNodeType[F]] = streamNodes[F]()
  }
}
