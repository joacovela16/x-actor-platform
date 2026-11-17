package xplat.flow.http

import cats.Functor
import cats.syntax.functor._
import io.circe.Json
import xplat.flow.engine.{CronScheduler, Origin}
import xplat.flow.model.CronExpression
import xplat.flow.nodes.CronNode

/** The schedules that the `cron` triggers make: each one starts an instance of its flow whenever its expression matches. Unlike the
  * cron nodes of a flow, a trigger is on as soon as it exists.
  */
object TriggerSchedules {

  def of(triggers: List[TriggerDef]): List[CronScheduler.Schedule] =
    triggers.filter(_.kind == TriggerDef.Kind.Cron).flatMap { t =>
      t.cron.flatMap(CronExpression.parse(_).toOption).map { expression =>
        CronScheduler.Schedule(
          key = s"trigger:${t.id}",
          flowId = t.flowId,
          expression = expression,
          timezone = t.timezone.getOrElse("UTC"),
          misfire = CronNode.FireOnce,
          inputs = Json.fromFields(t.inputs),
          origin = Origin.Trigger(t.id, None)
        )
      }
    }

  def provider[F[_]: Functor](registry: TriggerRegistry[F]): F[List[CronScheduler.Schedule]] = registry.list.map(of)
}
