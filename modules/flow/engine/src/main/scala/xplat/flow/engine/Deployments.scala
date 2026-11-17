package xplat.flow.engine

import cats.effect.{Ref, Sync}
import cats.syntax.all._

/** Which flows are switched on. A flow with a cron launcher only starts instances by the clock while it is enabled, so saving a
  * draft does not start launching it. It has nothing to do with whether the flow can be started by hand or by a trigger.
  */
trait Deployments[F[_]] {

  def enabled: F[Set[String]]

  def isEnabled(flowId: String): F[Boolean]

  def set(flowId: String, enabled: Boolean): F[Unit]
}

object Deployments {

  /** Keeps them in memory: what is enabled is lost when the process stops. */
  def inMemory[F[_]: Sync]: F[Deployments[F]] = Ref.of[F, Set[String]](Set.empty).map(fromRef(_))

  /** On top of any `Ref` that is saved when it changes, for stores that keep the set somewhere else. */
  private def fromRef[F[_]: Sync](ref: Ref[F, Set[String]]): Deployments[F] = new Deployments[F] {
    def enabled: F[Set[String]] = ref.get
    def isEnabled(flowId: String): F[Boolean] = ref.get.map(_.contains(flowId))
    def set(flowId: String, on: Boolean): F[Unit] = ref.update(s => if (on) s + flowId else s - flowId)
  }
}
