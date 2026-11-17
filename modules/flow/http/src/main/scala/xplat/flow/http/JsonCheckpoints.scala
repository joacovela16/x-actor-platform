package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.syntax.all._
import xplat.flow.engine.Checkpoints

import java.nio.file.Path

/** [[Checkpoints]] that are kept in a file of the data directory, so after a restart the schedules know which times they missed. */
object JsonCheckpoints {

  def make[F[_]: Async](file: Option[Path]): F[Checkpoints[F]] = inSlot(file.map(JsonSlot.file[F](_)))

  /** The same, kept in any [[JsonSlot]]. */
  def inSlot[F[_]: Async](slot: Option[JsonSlot[F]]): F[Checkpoints[F]] =
    for {
      initial <- slot.fold(Async[F].pure(Map.empty[String, Long]))(_.read[Map[String, Long]](Map.empty))
      ref <- Ref.of[F, Map[String, Long]](initial)
      lock <- Semaphore[F](1)
    } yield new Checkpoints[F] {
      private def change(f: Map[String, Long] => Map[String, Long]): F[Unit] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            val next = f(all)
            if (next == all) Async[F].unit else slot.traverse_(_.write[Map[String, Long]](next)) >> ref.set(next)
          }
        }

      def get(key: String): F[Option[Long]] = ref.get.map(_.get(key))
      def set(key: String, time: Long): F[Unit] = change(_.updated(key, time))
      def delete(key: String): F[Unit] = change(_ - key)
    }
}
