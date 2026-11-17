package xplat.flow.nodes

import cats.Applicative
import xplat.flow.model.NodeCatalog

/** The nodes that come with the core of the flow module. */
object CoreNodes {

  /** What the nodes are, which is all that validation and the editor need. */
  val catalog: NodeCatalog = new NodeCatalog(List(IfNode.descriptor, SubflowNode.descriptor, RespondNode.descriptor, CronNode.descriptor))

  /** The nodes that can run: `runner` is what the nodes that call other flows use, and `nextFire` what the cron node uses to know
    * when to go on (the engine passes one made with `java.time`).
    */
  def registry[F[_]: Applicative](runner: FlowRunner[F], nextFire: CronNode.NextFire = CronNode.NoClock): NodeRegistry[F] =
    NodeRegistry[F](new IfNode[F], new SubflowNode[F](runner), new RespondNode[F], new CronNode[F](nextFire))
}
