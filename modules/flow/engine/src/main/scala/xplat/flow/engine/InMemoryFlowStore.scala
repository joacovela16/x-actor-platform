package xplat.flow.engine

import cats.effect.{Ref, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._
import xplat.flow.model.{FlowDef, FlowError, FlowResolver, FlowValidator, NodeCatalog, Types}

/** A [[FlowStore]] that keeps everything in memory. */
final class InMemoryFlowStore[F[_]: Sync] private (catalog: NodeCatalog, flows: Ref[F, Map[String, Vector[FlowDef]]], types: F[Types])
    extends FlowStore[F] {

  def save(flow: FlowDef): F[Either[List[FlowError], FlowDef]] =
    types.flatMap(t => flows.modify { all =>
      val saved = flow.copy(revision = all.get(flow.id).flatMap(_.lastOption).fold(1)(_.revision + 1))
      // The new revision takes part in the check, so a flow that calls itself is found.
      val seen = all.updated(saved.id, all.getOrElse(saved.id, Vector.empty) :+ saved)
      FlowValidator.validate(saved, catalog, InMemoryFlowStore.resolverOf(seen), t) match {
        case Nil    => (seen, Right(saved))
        case errors => (all, Left(errors))
      }
    })

  def get(id: String, revision: Option[Int]): F[Option[FlowDef]] =
    flows.get.map(all => InMemoryFlowStore.resolverOf(all).find(id, revision))

  def list: F[List[FlowSummary]] =
    flows.get.map(_.values.flatMap(_.lastOption).map(f => FlowSummary(f.id, f.name, f.revision)).toList.sortBy(_.id))

  def revisions(id: String): F[List[FlowDef]] = flows.get.map(_.get(id).fold(List.empty[FlowDef])(_.toList))

  def delete(id: String): F[Unit] = flows.update(_ - id)

  def resolver: F[FlowResolver] = flows.get.map(InMemoryFlowStore.resolverOf)
}

object InMemoryFlowStore {

  def make[F[_]: Sync](catalog: NodeCatalog): F[InMemoryFlowStore[F]] = make(catalog, Sync[F].pure(Types.empty))

  /** The same, validating the types of the flows against these entities. */
  def make[F[_]: Sync](catalog: NodeCatalog, types: F[Types]): F[InMemoryFlowStore[F]] =
    Ref.of[F, Map[String, Vector[FlowDef]]](Map.empty).map(new InMemoryFlowStore[F](catalog, _, types))

  private def resolverOf(all: Map[String, Vector[FlowDef]]): FlowResolver = FlowResolver.of(all.values.flatten)
}
