package xplat.flow.engine

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Topic
import io.circe.parser.parse
import xplat.core.persistence.api.Journal
import xplat.core.persistence.api.journal.Shard

import java.nio.charset.StandardCharsets.UTF_8

/** Hands the audit entries of the instances to whoever is following them: all the ones so far, then the new ones as they
  * come. The entries of an instance that this process did not run are made from its journal the first time they are asked
  * for, so they are the same ones.
  */
private[engine] final class EventHub[F[_]] private (
    journal: Journal[F],
    redactor: Redactor,
    entries: Ref[F, Map[String, EventHub.Entry[F]]],
    mutex: Semaphore[F],
    observer: Option[FlowObserver[F]]
)(implicit F: Async[F]) {
  import EventHub.Entry

  private def entry(id: String): F[Entry[F]] =
    entries.get.map(_.get(id)).flatMap {
      case Some(e) => F.pure(e)
      case None =>
        mutex.permit.use { _ =>
          entries.get.map(_.get(id)).flatMap {
            case Some(e) => F.pure(e)
            case None =>
              for {
                past <- load(id)
                log <- Ref.of[F, Vector[AuditEntry]](past)
                topic <- Topic[F, AuditEntry]
                e = Entry(log, topic)
                _ <- entries.update(_ + (id -> e))
              } yield e
          }
        }
    }

  private def load(id: String): F[Vector[AuditEntry]] =
    journal
      .getEvents(Shard(InstanceActor.PersistenceName, id))
      .flatMap(_.compile.toVector)
      .flatMap(_.traverse(e => F.fromEither(parse(new String(e.data, UTF_8)).flatMap(_.as[InstanceEvent]))))
      .map(events => Audit.fromEvents(id, events, redactor))

  /** Adds entries that are not numbered yet, which get the next numbers. */
  def publish(id: String, entries: List[AuditEntry]): F[Unit] =
    if (entries.isEmpty) F.unit
    else
      entry(id).flatMap { e =>
        entries.traverse_ { entry =>
          e.log.modify { v => val numbered = entry.withSeq(v.size.toLong); (v :+ numbered, numbered) }.flatMap { numbered =>
            e.topic.publish1(numbered) >> observer.traverse_(_.observe(id, numbered).attempt.void)
          }
        }
      }

  /** Every entry so far. */
  def entries(id: String): F[Vector[AuditEntry]] = entry(id).flatMap(_.log.get)

  /** Every event so far and then the new ones. It ends with the event that finishes the instance. */
  def stream(id: String): Stream[F, AuditEntry] =
    Stream.eval(entry(id)).flatMap { e =>
      Stream.resource(e.topic.subscribeAwait(1024)).flatMap { live =>
        Stream.eval(e.log.get).flatMap { past =>
          val replay = Stream.emits(past)
          if (past.lastOption.exists(_.isInstanceOf[AuditEntry.InstanceFinished])) replay
          else replay ++ live.filter(_.seq >= past.size).takeThrough(!_.isInstanceOf[AuditEntry.InstanceFinished])
        }
      }
    }
}

private[engine] object EventHub {

  final case class Entry[F[_]](log: Ref[F, Vector[AuditEntry]], topic: Topic[F, AuditEntry])

  def make[F[_]: Async](journal: Journal[F], redactor: Redactor, observer: Option[FlowObserver[F]] = None): F[EventHub[F]] =
    for {
      entries <- Ref.of[F, Map[String, Entry[F]]](Map.empty)
      mutex <- Semaphore[F](1)
    } yield new EventHub[F](journal, redactor, entries, mutex, observer)
}
