package xplat.flow.engine

import cats.effect.{Async, Ref, Resource}
import cats.syntax.all._
import io.circe.Json
import org.typelevel.log4cats.slf4j.Slf4jLogger
import xplat.flow.model.CronExpression

import java.time.Instant
import scala.concurrent.duration._

/** Starts instances by the clock: for each [[CronScheduler.Schedule]] it starts an instance of the flow at every time its
  * expression matches.
  *
  *   - The id of the instance is `<key>:<time in milliseconds>`, so a time that is dealt with twice (the server stopped before it
  *     saved that it had, or two servers share the journal) starts nothing the second time.
  *   - Times that passed while the server was stopped are `misfire`: with `fire-once` one instance is started for them (the latest);
  *     with `skip` none. A time that is only a few seconds late (`grace`) is not a misfire. A schedule that is seen for the first
  *     time starts from now: it does not run the past.
  *   - The schedules are asked for every poll, so flows can be enabled, disabled, edited or deleted while it runs. A schedule that
  *     goes away is forgotten, so enabling it again later does not make up for the time it was off.
  */
object CronScheduler {

  /** @param key
    *   names the schedule and the instances it starts: `<flowId>:<nodeId>` for a cron node, the id of the trigger for a cron trigger.
    * @param misfire
    *   [[xplat.flow.nodes.CronNode.FireOnce]] or [[xplat.flow.nodes.CronNode.Skip]].
    * @param inputs
    *   the inputs of the flow for the instances that it starts.
    */
  final case class Schedule(key: String, flowId: String, expression: CronExpression, timezone: String, misfire: String, inputs: Json, origin: Origin)

  final case class Settings(pollEvery: FiniteDuration = 1.second, grace: FiniteDuration = 5.seconds, maxPerPoll: Int = 1000)

  /** The id of the instance that the schedule starts for the time `tick`. */
  def instanceId(schedule: Schedule, tick: Instant): String = s"${schedule.key}:${tick.toEpochMilli}"

  /** Runs in the background while the resource is in use, looking at the clock every `pollEvery`. */
  def resource[F[_]](
      engine: FlowEngine[F],
      schedules: F[List[Schedule]],
      checkpoints: Checkpoints[F],
      settings: Settings = Settings()
  )(implicit A: Async[F]): Resource[F, Unit] =
    for {
      poller <- Resource.eval(this.poller(engine, schedules, checkpoints, settings))
      _ <- A.background(poller.forever)
    } yield ()

  /** The same without a clock of its own: `pollAt` is told what time it is, which is how it is tested. */
  def poller[F[_]](
      engine: FlowEngine[F],
      schedules: F[List[Schedule]],
      checkpoints: Checkpoints[F],
      settings: Settings = Settings()
  )(implicit A: Async[F]): F[Poller[F]] =
    for {
      log <- Slf4jLogger.fromName[F]("xplat.flow.cron")
      known <- Ref.of[F, Set[String]](Set.empty)
    } yield new Poller[F](engine, schedules, checkpoints, settings, log, known)

  final class Poller[F[_]] private[CronScheduler] (
      engine: FlowEngine[F],
      schedules: F[List[Schedule]],
      checkpoints: Checkpoints[F],
      settings: Settings,
      log: org.typelevel.log4cats.Logger[F],
      known: Ref[F, Set[String]]
  )(implicit A: Async[F]) {

    private[CronScheduler] def forever: F[Unit] =
      A.realTime.map(d => Instant.ofEpochMilli(d.toMillis)).flatMap(pollAt).handleErrorWith(e => log.error(e)("cannot run the cron schedules")) >>
        A.sleep(settings.pollEvery) >> forever

    /** One look at the clock: deals with every time that has come since the last one. */
    def pollAt(now: Instant): F[Unit] =
      for {
        current <- schedules
        keys = current.map(_.key).toSet
        gone <- known.modify(k => (keys, k -- keys))
        _ <- gone.toList.traverse_(checkpoints.delete)
        _ <- current.traverse_(s => deal(s, now).handleErrorWith(e => log.error(e)(s"cannot start the instances of '${s.key}'")))
      } yield ()

    private def deal(s: Schedule, now: Instant): F[Unit] =
      CronSchedule.zone(s.timezone) match {
        case Left(problem) => log.warn(s"the schedule '${s.key}' is not run: $problem")
        case Right(zone) =>
          checkpoints.get(s.key).flatMap {
            // First time: it counts from now.
            case None => checkpoints.set(s.key, now.toEpochMilli)
            case Some(last) =>
              val due = CronSchedule.ticks(s.expression, zone, Instant.ofEpochMilli(last), now, settings.maxPerPoll)
              if (due.isEmpty) A.unit
              else {
                val (late, onTime) = due.partition(t => now.toEpochMilli - t.toEpochMilli > settings.grace.toMillis)
                val fire = (if (s.misfire == xplat.flow.nodes.CronNode.Skip) Nil else late.lastOption.toList) ++ onTime
                fire.traverse_(start(s, _)) >> checkpoints.set(s.key, due.last.toEpochMilli) >>
                  A.whenA(late.nonEmpty)(log.warn(s"'${s.key}': ${late.size} time(s) were missed (${s.misfire}), the last one was ${late.last}"))
              }
          }
      }

    /** Starts the instance. A flow that cannot be started the way the schedule says (it does not exist any more, its inputs do not fit)
      * would not be started by trying again, so that time is given up and logged; any other failure is raised and tried again.
      */
    private def start(s: Schedule, tick: Instant): F[Unit] =
      engine.start(s.flowId, None, s.inputs, Some(instanceId(s, tick)), s.origin).void.recoverWith { case e: FlowEngine.Error =>
        log.error(s"the schedule '${s.key}' cannot start the flow '${s.flowId}' for $tick: ${e.getMessage}")
      }
  }
}
