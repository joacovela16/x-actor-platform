package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.syntax.all._
import io.circe.Json
import xplat.flow.nodes.StateStore

/** A [[StateStore]] in memory and, with a slot, there too (a file of the data directory, or MongoDB). The whole map is written on
  * each change, which suits what it keeps: a few small snapshots, written every few seconds.
  */
object JsonStateStore {

  def inSlot[F[_]: Async](slot: Option[JsonSlot[F]]): F[StateStore[F]] =
    for {
      initial <- slot.fold(Async[F].pure(Map.empty[String, Json]))(_.read[Map[String, Json]](Map.empty))
      ref <- Ref.of[F, Map[String, Json]](initial)
      lock <- Semaphore[F](1)
    } yield new StateStore[F] {
      private def change(f: Map[String, Json] => Map[String, Json]): F[Unit] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            val next = f(all)
            if (next == all) Async[F].unit else slot.traverse_(_.write(next)) >> ref.set(next)
          }
        }

      def get(key: String): F[Option[Json]] = ref.get.map(_.get(key))
      def put(key: String, value: Json): F[Unit] = change(_.updated(key, value))
      def delete(key: String): F[Unit] = change(_ - key)
    }
}
