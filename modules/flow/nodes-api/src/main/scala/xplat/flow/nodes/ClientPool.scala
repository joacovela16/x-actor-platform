package xplat.flow.nodes

import cats.effect.kernel.{Deferred, Ref, Resource, Temporal}
import cats.effect.syntax.all._
import cats.syntax.all._

import scala.concurrent.duration._

/** The clients that nodes use to talk to other systems (a MongoDB client, a JDBC pool, a publisher...), shared by every node and
  * instance of the server. Opening one is expensive, so each is opened once and lent to whoever needs it.
  *
  *  - The key of a client is what it was opened with: the [[ClientPool.Kind]], the resolved connection (kind, parameters and
  *    secrets, after `${env.*}`) and an extra part that the kind chooses (a destination, for a publisher). Uses with the same key
  *    share one client; a changed environment variable gives another key, and the old client just becomes idle.
  *  - A client is opened the first time it is asked for, once even when many ask at the same time. If opening fails, those uses
  *    fail and nothing is kept: the next use tries again.
  *  - [[ClientPool.use]] lends it for the time of the operation. A client that is lent is never closed.
  *  - It is closed when it was not used for `idle`; when there are `max`, the least recently used idle one is closed to open a new
  *    one. When all are lent, a new one waits up to `acquireTimeout`.
  *  - [[ClientPool.retire]] (a connection was edited or deleted) closes the clients of a connection as soon as they are returned;
  *    the next use opens a new one with the new configuration.
  *  - When the resource is released, it waits up to `shutdownGrace` for the operations that are running and closes everything.
  */
trait ClientPool[F[_]] {

  /** Lends the client of `kind` for this connection while `f` runs. */
  def use[C, A](kind: ClientPool.Kind[F, C], connection: Connections.Resolved, extra: String = "")(f: C => F[A]): F[A]

  /** The clients of a connection are closed once they are returned; new uses open new ones. */
  def retire(connection: String): F[Unit]

  /** Closes now the clients of a connection that are not lent, and retires the others. */
  def close(connection: String): F[Unit] = retire(connection)

  /** The clients that are open now. */
  def clients: F[List[ClientPool.Client]]
}

object ClientPool {

  /** How to open and close one kind of client. `id` tells kinds apart in the key and in what is shown. */
  trait Kind[F[_], C] {
    def id: String
    def open(connection: Connections.Resolved, extra: String): Resource[F, C]
  }

  final case class Config(
      idle: FiniteDuration = 10.minutes,
      max: Int = 50,
      acquireTimeout: FiniteDuration = 30.seconds,
      shutdownGrace: FiniteDuration = 15.seconds
  )

  /** A client as the API shows it: never its configuration (it has secrets), only what tells it apart. */
  final case class Client(connection: String, kind: String, extra: String, inUse: Int, idleMillis: Long, retired: Boolean, opening: Boolean)

  final class PoolFull(max: Int) extends RuntimeException(s"there are already $max clients open and all of them are in use")

  private final case class Key(kind: String, connection: String, fingerprint: String, extra: String)

  private type Opened[F[_]] = Either[Throwable, (Any, F[Unit])]

  private final case class Entry[F[_]](key: Key, ready: Deferred[F, Opened[F]], opened: Boolean, borrowers: Int, lastUsed: Long, retired: Boolean)

  /** `active` finds the entry that new uses of a key get; `entries` has also the retired ones that are still lent. */
  private final case class State[F[_]](active: Map[Key, Long], entries: Map[Long, Entry[F]], next: Long) {
    def remove(id: Long): State[F] =
      entries.get(id).fold(this)(e => copy(active = if (active.get(e.key).contains(id)) active - e.key else active, entries = entries - id))
  }

  /** What a client was opened with, as a short text: secrets are part of it but cannot be read back from it. (MurmurHash3, which
    * is enough to tell configurations apart and works on every platform.)
    */
  private def fingerprint(c: Connections.Resolved): String = {
    val text = c.kind + "\u0000" + c.params.noSpaces + "\u0000" + c.secrets.toList.sorted.mkString("\u0000")
    val h = scala.util.hashing.MurmurHash3
    f"${h.stringHash(text, 1)}%08x${h.stringHash(text, 2)}%08x${h.stringHash(text, 3)}%08x"
  }

  def make[F[_]](config: Config = Config())(implicit F: Temporal[F]): Resource[F, ClientPool[F]] =
    Resource
      .make(Ref.of[F, State[F]](State(Map.empty, Map.empty, 0L)))(state => shutdown(state, config))
      .flatMap { state =>
        val pool = new Impl[F](state, config)
        val every = (config.idle / 4).min(30.seconds).max(10.millis)
        (F.sleep(every) >> pool.reap).foreverM.background.as(pool: ClientPool[F])
      }

  private def shutdown[F[_]](state: Ref[F, State[F]], config: Config)(implicit F: Temporal[F]): F[Unit] = {
    val quiet = state.get.map(_.entries.values.forall(_.borrowers == 0))
    (F.sleep(20.millis) >> quiet).iterateUntil(identity).void.timeoutTo(config.shutdownGrace, F.unit) >>
      state.getAndSet(State(Map.empty, Map.empty, 0L)).flatMap(_.entries.values.toList.traverse_(closeEntry(_)))
  }

  /** Closes what an entry opened; one that is still opening is closed by whoever opens it, when it sees it is gone. */
  private def closeEntry[F[_]](e: Entry[F])(implicit F: Temporal[F]): F[Unit] =
    if (!e.opened) F.unit else e.ready.get.flatMap(_.toOption.traverse_(_._2.attempt.void))

  private final class Impl[F[_]](state: Ref[F, State[F]], config: Config)(implicit F: Temporal[F]) extends ClientPool[F] {

    private val now: F[Long] = F.realTime.map(_.toMillis)

    private sealed trait Step
    private final case class Borrow(id: Long, e: Entry[F]) extends Step
    private final case class Open(id: Long, e: Entry[F], evicted: Option[Entry[F]]) extends Step
    private case object Full extends Step

    def use[C, A](kind: Kind[F, C], connection: Connections.Resolved, extra: String)(f: C => F[A]): F[A] = {
      val key = Key(kind.id, connection.name, fingerprint(connection), extra)

      val attempt: F[Option[(Long, Entry[F])]] =
        F.deferred[Opened[F]].flatMap { fresh =>
          now.flatMap(t => state.modify(s => step(s, key, fresh, t))).flatMap {
            case Borrow(id, e) => F.pure(Some(id -> e))
            case Full          => F.sleep(20.millis).as(None)
            case Open(id, e, evicted) =>
              evicted.traverse_(closeEntry(_)) >>
                kind.open(connection, extra).allocated.attempt.flatMap { result =>
                  val value: Opened[F] = result.map { case (c, close) => (c: Any, close) }
                  e.ready.complete(value) >> (value match {
                    case Left(error) => state.update(_.remove(id)) >> F.raiseError[Option[(Long, Entry[F])]](error)
                    case Right((_, close)) =>
                      state
                        .modify { s =>
                          s.entries.get(id) match {
                            case Some(now) => (s.copy(entries = s.entries.updated(id, now.copy(opened = true))), true)
                            case None      => (s, false) // the pool was shut down while it was opening
                          }
                        }
                        .flatMap(kept => if (kept) F.pure(Some(id -> e)) else close.attempt >> F.raiseError(new IllegalStateException("the pool is closed")))
                  })
                }
          }
        }

      val acquire = attempt.untilDefinedM.timeoutTo(config.acquireTimeout, F.raiseError(new PoolFull(config.max)))

      F.uncancelable { poll =>
        poll(acquire).flatMap { case (id, e) =>
          poll(e.ready.get.rethrow.flatMap { case (client, _) => f(client.asInstanceOf[C]) }).guarantee(release(id))
        }
      }
    }

    /** Borrow what is there, open a new one (closing an idle one if there are too many), or wait. */
    private def step(s: State[F], key: Key, fresh: Deferred[F, Opened[F]], t: Long): (State[F], Step) =
      s.active.get(key).flatMap(id => s.entries.get(id).map(id -> _)) match {
        case Some((id, e)) =>
          val lent = e.copy(borrowers = e.borrowers + 1, lastUsed = t)
          (s.copy(entries = s.entries.updated(id, lent)), Borrow(id, lent))
        case None =>
          val id = s.next
          val created = Entry[F](key, fresh, opened = false, borrowers = 1, lastUsed = t, retired = false)
          def add(from: State[F]) = from.copy(active = from.active.updated(key, id), entries = from.entries.updated(id, created), next = id + 1)
          if (s.entries.size < config.max) (add(s), Open(id, created, None))
          else
            s.entries.filter { case (_, e) => e.borrowers == 0 && e.opened }.minByOption(_._2.lastUsed) match {
              case Some((victimId, victim)) => (add(s.remove(victimId)), Open(id, created, Some(victim)))
              case None                     => (s, Full)
            }
      }

    private def release(id: Long): F[Unit] =
      now.flatMap { t =>
        state.modify { s =>
          s.entries.get(id) match {
            case None => (s, None)
            case Some(e) =>
              val back = e.copy(borrowers = e.borrowers - 1, lastUsed = t)
              if (back.retired && back.borrowers == 0) (s.remove(id), Some(back)) else (s.copy(entries = s.entries.updated(id, back)), None)
          }
        }.flatMap(_.traverse_(closeEntry(_)))
      }

    def retire(connection: String): F[Unit] =
      state.modify { s =>
        val mine = s.entries.filter(_._2.key.connection == connection)
        val (idle, lent) = mine.partition(_._2.borrowers == 0)
        val next = s.copy(
          active = s.active.filterNot(_._1.connection == connection),
          entries = s.entries -- idle.keys ++ lent.map { case (id, e) => id -> e.copy(retired = true) }
        )
        (next, idle.values.toList)
      }.flatMap(_.traverse_(closeEntry(_)))

    def reap: F[Unit] =
      now.flatMap { t =>
        state.modify { s =>
          val old = s.entries.filter { case (_, e) => e.opened && e.borrowers == 0 && t - e.lastUsed >= config.idle.toMillis }
          (old.keys.foldLeft(s)(_ remove _), old.values.toList)
        }.flatMap(_.traverse_(closeEntry(_)))
      }

    def clients: F[List[Client]] =
      now.flatMap { t =>
        state.get.map(_.entries.values.toList.map { e =>
          Client(e.key.connection, e.key.kind, e.key.extra, e.borrowers, if (e.borrowers > 0) 0L else t - e.lastUsed, e.retired, !e.opened)
        }.sortBy(c => (c.connection, c.kind, c.extra)))
      }
  }
}
