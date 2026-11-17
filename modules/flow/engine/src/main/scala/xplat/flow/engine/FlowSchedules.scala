package xplat.flow.engine

import cats.Monad
import cats.syntax.all._
import xplat.flow.model.{CronExpression, FlowDef}
import xplat.flow.nodes.CronNode

/** The schedules that the flows have: one for each cron node that is where the execution starts (it has no incoming edge). */
object FlowSchedules {

  /** The schedules of the latest revision of every flow that is enabled. */
  def enabled[F[_]: Monad](store: FlowStore[F], deployments: Deployments[F]): F[List[CronScheduler.Schedule]] =
    deployments.enabled.flatMap(ids => ids.toList.sorted.traverse(store.get(_, None))).map(_.flatten.flatMap(of))

  /** The schedules of a flow, whether it is enabled or not. A node with an expression that is wrong is left out: saving the flow
    * already refused it.
    */
  def of(flow: FlowDef): List[CronScheduler.Schedule] =
    flow.nodes.filter(n => n.kind == CronNode.Kind && !flow.edges.exists(_.to == n.id)).flatMap { node =>
      for {
        params <- CronNode.params(node.params).toOption
        expression <- CronExpression.parse(params.expression).toOption
      } yield CronScheduler.Schedule(
        key = s"${flow.id}:${node.id}",
        flowId = flow.id,
        expression = expression,
        timezone = params.timezone,
        misfire = params.misfire,
        inputs = params.inputs,
        origin = Origin.Cron(s"${flow.id}/${node.id}")
      )
    }
}
