package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.syntax.all._
import xplat.flow.engine.Deployments

import java.nio.file.Path

/** [[Deployments]] that are kept in a file of the data directory, so a flow that was enabled still is after a restart. */
object JsonDeployments {

  def make[F[_]: Async](file: Option[Path]): F[Deployments[F]] = inSlot(file.map(JsonSlot.file[F](_)))

  /** The same, kept in any [[JsonSlot]]. */
  def inSlot[F[_]: Async](slot: Option[JsonSlot[F]]): F[Deployments[F]] =
    for {
      initial <- slot.fold(Async[F].pure(List.empty[String]))(_.read[List[String]](Nil))
      ref <- Ref.of[F, Set[String]](initial.toSet)
      lock <- Semaphore[F](1)
    } yield new Deployments[F] {
      def enabled: F[Set[String]] = ref.get
      def isEnabled(flowId: String): F[Boolean] = ref.get.map(_.contains(flowId))
      def set(flowId: String, on: Boolean): F[Unit] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            val next = if (on) all + flowId else all - flowId
            if (next == all) Async[F].unit else slot.traverse_(_.write[List[String]](next.toList.sorted)) >> ref.set(next)
          }
        }
    }
}
