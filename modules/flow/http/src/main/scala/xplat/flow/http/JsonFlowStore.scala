package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._
import xplat.flow.engine.{FlowStore, FlowSummary}
import xplat.flow.model.{FlowDef, FlowError, FlowResolver, FlowValidator, NodeCatalog, Types}

import java.nio.file.Path

/** A [[FlowStore]] that keeps the flows as one JSON value (a file, see [[JsonSlot]]), every revision of each, and reads it when it
  * starts. Without a slot it keeps them in memory only.
  */
final class JsonFlowStore[F[_]: Sync] private (
    catalog: NodeCatalog,
    slot: Option[JsonSlot[F]],
    flows: Ref[F, Map[String, Vector[FlowDef]]],
    lock: Semaphore[F],
    types: F[Types]
) extends FlowStore[F] {

  private def resolverOf(all: Map[String, Vector[FlowDef]]): FlowResolver = FlowResolver.of(all.values.flatten)

  private def persist(all: Map[String, Vector[FlowDef]]): F[Unit] =
    slot.traverse_(_.write[List[FlowDef]](all.values.flatten.toList.sortBy(f => (f.id, f.revision))))

  def save(flow: FlowDef): F[Either[List[FlowError], FlowDef]] =
    lock.permit.use { _ =>
      (flows.get, types).tupled.flatMap { case (all, t) =>
        val saved = flow.copy(revision = all.get(flow.id).flatMap(_.lastOption).fold(1)(_.revision + 1))
        // The new revision takes part in the check, so a flow that calls itself is found.
        val next = all.updated(saved.id, all.getOrElse(saved.id, Vector.empty) :+ saved)
        FlowValidator.validate(saved, catalog, resolverOf(next), t) match {
          case Nil    => persist(next) >> flows.set(next).as(Right(saved))
          case errors => Sync[F].pure(Left(errors))
        }
      }
    }

  def get(id: String, revision: Option[Int]): F[Option[FlowDef]] = flows.get.map(all => resolverOf(all).find(id, revision))

  def list: F[List[FlowSummary]] =
    flows.get.map(_.values.flatMap(_.lastOption).map(f => FlowSummary(f.id, f.name, f.revision)).toList.sortBy(_.id))

  def revisions(id: String): F[List[FlowDef]] = flows.get.map(_.get(id).fold(List.empty[FlowDef])(_.toList))

  def delete(id: String): F[Unit] =
    lock.permit.use(_ => flows.get.flatMap { all => val next = all - id; persist(next) >> flows.set(next) })

  def resolver: F[FlowResolver] = flows.get.map(resolverOf)
}

object JsonFlowStore {

  def make[F[_]: Async](catalog: NodeCatalog, file: Option[Path] = None): F[JsonFlowStore[F]] = inSlot(catalog, file.map(JsonSlot.file[F](_)))

  /** The same, kept in any [[JsonSlot]]. */
  def inSlot[F[_]: Async](catalog: NodeCatalog, slot: Option[JsonSlot[F]]): F[JsonFlowStore[F]] = inSlot(catalog, slot, Sync[F].pure(Types.empty))

  /** The same, validating the types of the flows against the entities `types` gives each time. */
  def inSlot[F[_]: Async](catalog: NodeCatalog, slot: Option[JsonSlot[F]], types: F[Types]): F[JsonFlowStore[F]] =
    for {
      initial <- slot.fold(Sync[F].pure(List.empty[FlowDef]))(_.read[List[FlowDef]](Nil))
      byId = initial.groupBy(_.id).map { case (id, revisions) => id -> revisions.sortBy(_.revision).toVector }
      ref <- Ref.of[F, Map[String, Vector[FlowDef]]](byId)
      lock <- Semaphore[F](1)
    } yield new JsonFlowStore[F](catalog, slot, ref, lock, types)
}
