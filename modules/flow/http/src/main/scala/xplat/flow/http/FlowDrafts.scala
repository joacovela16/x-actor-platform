package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.syntax.all._
import xplat.flow.model.FlowDef

/** The flow that is being edited and is not published yet: at most one per flow id. It is saved as it is, without validating it, so
  * work that does not validate yet is not lost. Publishing it validates it and makes it a revision of the [[xplat.flow.engine.FlowStore]];
  * only published revisions run.
  */
trait FlowDrafts[F[_]] {
  def get(id: String): F[Option[FlowDef]]
  def all: F[List[FlowDef]]
  def put(flow: FlowDef): F[Unit]
  def delete(id: String): F[Unit]
}

object FlowDrafts {

  /** Kept in memory and, with a slot, there too (a file of the data directory, or MongoDB). */
  def inSlot[F[_]: Async](slot: Option[JsonSlot[F]]): F[FlowDrafts[F]] =
    for {
      initial <- slot.fold(Async[F].pure(List.empty[FlowDef]))(_.read[List[FlowDef]](Nil))
      ref <- Ref.of[F, Map[String, FlowDef]](initial.map(f => f.id -> f).toMap)
      lock <- Semaphore[F](1)
    } yield new FlowDrafts[F] {
      private def change(f: Map[String, FlowDef] => Map[String, FlowDef]): F[Unit] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            val next = f(all)
            if (next == all) Async[F].unit else slot.traverse_(_.write[List[FlowDef]](next.values.toList.sortBy(_.id))) >> ref.set(next)
          }
        }

      def get(id: String): F[Option[FlowDef]] = ref.get.map(_.get(id))
      def all: F[List[FlowDef]] = ref.get.map(_.values.toList.sortBy(_.id))
      def put(flow: FlowDef): F[Unit] = change(_.updated(flow.id, flow))
      def delete(id: String): F[Unit] = change(_ - id)
    }

  /** Only in memory. */
  def inMemory[F[_]: Async]: F[FlowDrafts[F]] = inSlot[F](None)
}
