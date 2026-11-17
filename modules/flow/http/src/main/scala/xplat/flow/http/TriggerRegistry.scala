package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._

import java.nio.file.Path

/** The triggers that exist, and the one that serves a request. */
trait TriggerRegistry[F[_]] {

  def list: F[List[TriggerDef]]

  def get(id: String): F[Option[TriggerDef]]

  /** Saves the trigger, replacing the one with its id. It is refused, with the reasons, if it is badly formed or if another
    * trigger already answers the same method and path.
    */
  def put(trigger: TriggerDef): F[Either[List[String], TriggerDef]]

  def delete(id: String): F[Boolean]

  /** The HTTP trigger that serves `method` and `path` (without `/hooks/`). */
  def route(method: String, path: String): F[Option[TriggerDef]]
}

object TriggerRegistry {

  /** Keeps the triggers in memory and, if there is a `file`, in it too: it is read when the registry starts and written when
    * a trigger changes.
    */
  def make[F[_]: Async](file: Option[Path] = None): F[TriggerRegistry[F]] = inSlot(file.map(JsonSlot.file[F](_)))

  /** The same, kept in any [[JsonSlot]]. */
  def inSlot[F[_]: Async](slot: Option[JsonSlot[F]]): F[TriggerRegistry[F]] =
    for {
      initial <- slot.fold(Sync[F].pure(List.empty[TriggerDef]))(_.read[List[TriggerDef]](Nil))
      ref <- Ref.of[F, Map[String, TriggerDef]](initial.map(t => t.id -> t).toMap)
      lock <- Semaphore[F](1)
    } yield new TriggerRegistry[F] {

      private def persist(all: Map[String, TriggerDef]): F[Unit] =
        slot.traverse_(_.write[List[TriggerDef]](all.values.toList.sortBy(_.id)))

      def list: F[List[TriggerDef]] = ref.get.map(_.values.toList.sortBy(_.id))

      def get(id: String): F[Option[TriggerDef]] = ref.get.map(_.get(id))

      def put(trigger: TriggerDef): F[Either[List[String], TriggerDef]] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            val clash = all.values.find { o =>
              o.id != trigger.id && trigger.kind == TriggerDef.Kind.Http && o.kind == TriggerDef.Kind.Http &&
              o.httpMethod == trigger.httpMethod && o.route == trigger.route
            }
            trigger.problems ++ clash.map(o => s"the trigger '${o.id}' already serves ${o.httpMethod} /hooks/${o.route}") match {
              case Nil =>
                val next = all.updated(trigger.id, trigger)
                persist(next) >> ref.set(next).as(Right(trigger))
              case problems => Sync[F].pure(Left(problems))
            }
          }
        }

      def delete(id: String): F[Boolean] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            if (all.contains(id)) { val next = all - id; persist(next) >> ref.set(next).as(true) }
            else Sync[F].pure(false)
          }
        }

      def route(method: String, path: String): F[Option[TriggerDef]] = {
        val wanted = path.split("/").filter(_.nonEmpty).mkString("/")
        ref.get.map(_.values.find(t => t.kind == TriggerDef.Kind.Http && t.httpMethod == method.toUpperCase && t.route == wanted))
      }
    }
}
