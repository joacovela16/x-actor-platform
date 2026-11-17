package xplat.flow.engine

import cats.effect.{Ref, Sync}
import cats.syntax.all._

/** The last time of each schedule that was dealt with, in milliseconds since the epoch, so that after a restart a scheduler knows
  * which times it missed. Saved by whoever stores the data of the server.
  */
trait Checkpoints[F[_]] {

  def get(key: String): F[Option[Long]]

  def set(key: String, time: Long): F[Unit]

  def delete(key: String): F[Unit]
}

object Checkpoints {

  def inMemory[F[_]: Sync]: F[Checkpoints[F]] =
    Ref.of[F, Map[String, Long]](Map.empty).map { ref =>
      new Checkpoints[F] {
        def get(key: String): F[Option[Long]] = ref.get.map(_.get(key))
        def set(key: String, time: Long): F[Unit] = ref.update(_.updated(key, time))
        def delete(key: String): F[Unit] = ref.update(_ - key)
      }
    }
}
