package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import xplat.flow.entity.{EntityCatalog, EntityDef, EntityError, EntityValidator}

/** The entities of the business model: those edited through the API, kept as one JSON value (a file, see [[JsonSlot]]) with every
  * revision of each, and those defined in code (the server's own or a plugin's), which cannot be edited or deleted. When an edited
  * entity and one of code have the same name, the one of code is used.
  */
final class EntityStore[F[_]: Sync] private (
    slot: Option[JsonSlot[F]],
    builtIn: Map[String, EntityStore.Fixed],
    stored: Ref[F, Map[String, Vector[EntityDef]]],
    lock: Semaphore[F]
) {

  import EntityStore._

  private def catalogOf(all: Map[String, Vector[EntityDef]]): EntityCatalog =
    EntityCatalog(all.values.flatMap(_.lastOption).toList.sortBy(_.name) ++ builtIn.values.map(_.entity).toList.sortBy(_.name))

  private def persist(all: Map[String, Vector[EntityDef]]): F[Unit] =
    slot.traverse_(_.write[List[EntityDef]](all.values.flatten.toList.sortBy(e => (e.name, e.revision))))

  /** The entities as they are now: the latest revision of each. */
  def catalog: F[EntityCatalog] = stored.get.map(catalogOf)

  def list: F[List[Summary]] =
    stored.get.map { all =>
      val edited = all.values.flatMap(_.lastOption).filterNot(e => builtIn.contains(e.name)).map(e => Summary.of(e, Source.Store)).toList
      (edited ++ builtIn.values.map(f => Summary.of(f.entity, f.source))).sortBy(_.name)
    }

  /** Every entity, whole, with where it comes from. */
  def all: F[List[(EntityDef, String)]] =
    stored.get.map { all =>
      val edited = all.values.flatMap(_.lastOption).filterNot(e => builtIn.contains(e.name)).map(_ -> Source.Store).toList
      (edited ++ builtIn.values.map(f => f.entity -> f.source)).sortBy(_._1.name)
    }

  /** The entity with that name and where it comes from. */
  def get(name: String): F[Option[(EntityDef, String)]] =
    stored.get.map(all => builtIn.get(name).map(f => (f.entity, f.source)).orElse(all.get(name).flatMap(_.lastOption).map(_ -> Source.Store)))

  /** The names of the entities edited through the API that one of code hides. */
  def shadowed: F[List[String]] = stored.get.map(_.keys.filter(builtIn.contains).toList.sorted)

  def revisions(name: String): F[List[EntityDef]] =
    stored.get.map(all => builtIn.get(name).fold(all.getOrElse(name, Vector.empty).toList)(f => List(f.entity)))

  /** What `entity` would be refused for, if it were saved now; empty when it can be. */
  def validate(entity: EntityDef): F[List[EntityError]] = catalog.map(EntityValidator.validate(entity, _))

  /** Saves `entity` as the next revision of its name, if it is valid among the others. */
  def save(entity: EntityDef): F[Either[Refused, EntityDef]] =
    lock.permit.use { _ =>
      builtIn.get(entity.name) match {
        case Some(fixed) => Sync[F].pure(Left(Refused.ReadOnly(entity.name, fixed.source)))
        case None =>
          stored.get.flatMap { all =>
            val saved = entity.copy(revision = all.get(entity.name).flatMap(_.lastOption).fold(1)(_.revision + 1))
            EntityValidator.validate(saved, catalogOf(all)) match {
              case Nil =>
                val next = all.updated(saved.name, all.getOrElse(saved.name, Vector.empty) :+ saved)
                persist(next) >> stored.set(next).as(Right(saved))
              case errors => Sync[F].pure(Left(Refused.Invalid(errors)))
            }
          }
      }
    }

  /** Deletes the entity and its revisions, unless it is of code or another entity holds or references it. */
  def delete(name: String): F[Either[Refused, Unit]] =
    lock.permit.use { _ =>
      stored.get.flatMap { all =>
        builtIn.get(name) match {
          case Some(fixed)                  => Sync[F].pure(Left(Refused.ReadOnly(name, fixed.source)))
          case None if !all.contains(name)  => Sync[F].pure(Left(Refused.NotFound(name)))
          case None =>
            catalogOf(all).usersOf(name) match {
              case Nil   => val next = all - name; persist(next) >> stored.set(next).as(Right(()))
              case users => Sync[F].pure(Left(Refused.InUse(name, users)))
            }
        }
      }
    }
}

object EntityStore {

  /** Where an entity comes from: `store` (edited through the API), `code` (given to the server) or `plugin:<name>`. */
  object Source {
    val Store: String = "store"
    val Code: String = "code"
    def plugin(name: String): String = s"plugin:$name"
  }

  /** An entity that is defined in code and cannot be changed through the API. */
  final case class Fixed(entity: EntityDef, source: String)

  final case class Summary(name: String, kind: EntityDef.Kind, label: Option[String], revision: Int, source: String, fields: Int)

  object Summary {
    def of(e: EntityDef, source: String): Summary = Summary(e.name, e.kind, e.label, e.revision, source, e.fields.size)

    implicit val encoder: Encoder[Summary] = Encoder.instance { s =>
      Json.obj(
        "name" -> s.name.asJson,
        "kind" -> s.kind.asJson,
        "label" -> s.label.asJson,
        "revision" -> s.revision.asJson,
        "source" -> s.source.asJson,
        "fields" -> s.fields.asJson
      )
    }
  }

  /** Why a change was not made. */
  sealed trait Refused

  object Refused {
    final case class ReadOnly(name: String, source: String) extends Refused
    final case class Invalid(errors: List[EntityError]) extends Refused
    final case class InUse(name: String, entities: List[String]) extends Refused
    final case class NotFound(name: String) extends Refused
  }

  def make[F[_]: Async](file: Option[java.nio.file.Path] = None, builtIn: List[Fixed] = Nil): F[EntityStore[F]] =
    inSlot(file.map(JsonSlot.file[F](_)), builtIn)

  /** The store kept in `slot` (in memory only without one), with the entities of code `builtIn`. Two of those with the same name
    * are a mistake of whoever builds the server.
    */
  def inSlot[F[_]: Async](slot: Option[JsonSlot[F]], builtIn: List[Fixed] = Nil): F[EntityStore[F]] = {
    val clashes = builtIn.groupBy(_.entity.name).collect { case (name, fs) if fs.size > 1 => s"'$name' (${fs.map(_.source).mkString(", ")})" }
    for {
      _ <- Sync[F].raiseError[Unit](new IllegalArgumentException(s"entities defined more than once: ${clashes.mkString(", ")}")).whenA(clashes.nonEmpty)
      initial <- slot.fold(Sync[F].pure(List.empty[EntityDef]))(_.read[List[EntityDef]](Nil))
      byName = initial.groupBy(_.name).map { case (name, revisions) => name -> revisions.sortBy(_.revision).toVector }
      ref <- Ref.of[F, Map[String, Vector[EntityDef]]](byName)
      lock <- Semaphore[F](1)
    } yield new EntityStore[F](slot, builtIn.map(f => f.entity.name -> f).toMap, ref, lock)
  }
}
