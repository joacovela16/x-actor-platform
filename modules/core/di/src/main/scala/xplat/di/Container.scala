package xplat.di

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Mutex
import cats.implicits._

import scala.collection.mutable

/** Dependency injection without annotations: a [[Key]] is both the id of a service and its type.
  *
  * {{{
  * val Log   = Key.single[Logger]("logger")
  * val Tools = Key.multiple[Tool]("tools")
  * val Agent = Key.single[Agent]("agent")
  *
  * Container[IO].use { c =>
  *   c.provide(
  *     Log.pure(consoleLogger()),
  *     Tools.dependsOn(Log).pure(log => readTool(log)),
  *     Tools.dependsOn(Log).pure(log => writeTool(log)),
  *     Agent.dependsOn(Log, Tools).resource((log, tools) => Agent.resource(log, tools))
  *   ) >> c.get(Agent)
  * }
  * }}}
  *
  * Each module can export its services as values and the composition stays in one place.
  *
  * Services are built once, on first use, in the order their dependencies need. A service built from a `Resource` is
  * released when its registration is removed, when something it depends on is removed, and when the container is
  * released (in reverse order of construction).
  *
  * Registrations can be removed at runtime. A multiple key resolves to a live [[Many]]: whoever received it sees the
  * services added and removed later, without being rebuilt. Removing the registration of a single key releases what
  * was built with it; it is built again on the next request.
  *
  * A [[scope]] is a child container for a request, a user or a month: it registers the evidence of that case (single
  * keys such as "pays Caja") and the parent's contributions to multiple keys are evaluated again with it. A
  * contribution that depends on evidence the scope lacks is not built, so "if there is such evidence, offer such
  * service" is declared in the dependencies, without separate conditions.
  *
  * {{{
  * val PaysCaja = Key.single[Unit]("pays-caja")
  * app.provide(Rows.dependsOn(PaysCaja).pure(_ => cajaRow))
  *
  * app.scope.use { scope =>
  *   scope.provide(PaysCaja.pure(())) >> scope.get(Rows).flatMap(_.current) // List(cajaRow); in `app`, Nil
  * }
  * }}}
  *
  * Factories must not call the container: they receive their dependencies. Operations are serialized, so a factory
  * that calls back into the container waits forever.
  */
trait Container[F[_]] {

  /** Registers the services. A single key accepts one registration; a multiple key any number, each adds one element.
    * If one fails (for example because building it for an already resolved multiple key fails), none stays registered.
    */
  def provide(services: Service[F]*): F[Container.Registration[F]]

  /** Builds (once) and returns the service. */
  def get[T](key: Key.Single[T]): F[T]

  /** The live list of services of the key; empty if none is registered. */
  def get[T](key: Key.Multiple[T]): F[Many[F, T]]

  /** Whether `key` can be resolved now: it has a registration (a multiple key always) and so do its dependencies. */
  def has(key: Key[_]): F[Boolean]

  /** The keys without registration that prevent resolving `key` (itself or a dependency); empty if it resolves. */
  def missing(key: Key[_]): F[List[Key[_]]]

  /** The keys with some registration, without repeating. In a scope, also those of its parents. */
  def registeredKeys: F[List[Key[_]]]

  /** Called after each registration or removal, with its key (in a scope, also those of its parents). */
  def onChange(listener: Key[_] => F[Unit]): F[Container.Registration[F]]

  /** A child container. Single keys it does not register come from the parent, with the same instance. The parent's
    * contributions to multiple keys are built again in the child, with their dependencies resolved in it first, before
    * the ones the child registers. It follows the parent's changes until released, which does not touch the parent.
    */
  def scope: Resource[F, Container[F]]
}

object Container {

  /** Undoes a registration or a subscription. */
  trait Registration[F[_]] {
    def remove: F[Unit]
  }

  /** @param onBuild
    *   called each time a service is built, after its dependencies.
    * @param onReleaseError
    *   called when releasing a service fails; the other services are released anyway. By default it prints to stderr.
    */
  final case class Options[F[_]](
      onBuild: Option[Key[_] => F[Unit]] = None,
      onReleaseError: Option[(Key[_], Throwable) => F[Unit]] = None
  )

  /** A service is missing, registered twice, or its dependencies form a cycle. */
  final case class DependencyError(message: String) extends RuntimeException(message)

  def apply[F[_]: Async]: Resource[F, Container[F]] = withOptions(Options[F]())

  def withOptions[F[_]](options: Options[F])(implicit A: Async[F]): Resource[F, Container[F]] =
    Resource.eval(Mutex[F]).flatMap { mutex =>
      val shared = new Shared[F](mutex, options)
      Resource.make(A.delay(new Impl[F](shared, None)))(c => shared.locked(c.disposeAll))
    }

  /** What every container of a tree shares: one lock, so a scope and its parent never interleave. */
  private final class Shared[F[_]](mutex: Mutex[F], val options: Options[F])(implicit A: Async[F]) {
    // Changes to report once the lock is released, so listeners can use the container.
    val pending: mutable.ArrayBuffer[(List[Key[_] => F[Unit]], Key[_])] = mutable.ArrayBuffer.empty

    def locked[A](fa: F[A]): F[A] =
      mutex.lock
        .surround(fa.flatMap(a => A.delay { val p = pending.toList; pending.clear(); (a, p) }).onError(_ => A.delay(pending.clear())))
        .flatMap { case (a, notifications) =>
          notifications.traverse_ { case (listeners, key) => listeners.traverse_(l => l(key).handleError(_ => ())) }.as(a)
        }
  }

  /** A registration: the key, its dependencies and how it is built. Compared by reference. */
  private final class Reg[F[_]](val service: Service[F]) {
    def key: Key[_] = service.key
    def deps: List[Key[_]] = service.deps
  }

  private final class Entry[F[_]](val reg: Reg[F]) {
    var built: Boolean = false
    var value: Any = null
    var release: Option[F[Unit]] = None
  }

  private final class LiveList[F[_], T](implicit A: Async[F]) extends Many[F, T] {
    @volatile var values: List[Any] = Nil
    def current: F[List[T]] = A.delay(values.asInstanceOf[List[T]])
  }

  private final class Impl[F[_]](shared: Shared[F], parent: Option[Impl[F]])(implicit A: Async[F]) extends Container[F] {
    // All the state below is only touched while holding the shared lock.
    private val entries = mutable.LinkedHashMap.empty[String, mutable.ArrayBuffer[Entry[F]]]
    // In a scope: what was built from the parent's contributions, by registration.
    private val inherited = mutable.LinkedHashMap.empty[Reg[F], Entry[F]]
    private val lists = mutable.LinkedHashMap.empty[String, LiveList[F, Any]]
    private val built = mutable.ArrayBuffer.empty[Entry[F]]
    private var resolving: List[Key[_]] = Nil
    private val listeners = mutable.ArrayBuffer.empty[Key[_] => F[Unit]]
    private val children = mutable.ArrayBuffer.empty[Impl[F]]

    private def delay[A](a: => A): F[A] = A.delay(a)

    // ---- reads, without building ----

    private def hasNow(key: Key[_], seen: Set[String] = Set.empty): Boolean =
      if (key.isMultiple) true
      else if (parent.isDefined && !entries.contains(key.name)) parent.get.hasNow(key)
      else if (seen(key.name)) false
      else entries.get(key.name).flatMap(_.headOption).exists(e => e.reg.deps.forall(hasNow(_, seen + key.name)))

    private def missingNow(key: Key[_], seen: Set[String] = Set.empty): List[Key[_]] =
      if (key.isMultiple || seen(key.name)) Nil
      else if (parent.isDefined && !entries.contains(key.name)) parent.get.missingNow(key)
      else
        entries.get(key.name).flatMap(_.headOption) match {
          case None        => List(key)
          case Some(entry) => entry.reg.deps.flatMap(missingNow(_, seen + key.name)).distinct
        }

    private def registeredNow: List[Key[_]] = {
      val own = entries.values.flatMap(_.headOption.map(_.reg.key)).toList
      val fromParent = parent.toList.flatMap(_.registeredNow).filterNot(k => entries.contains(k.name))
      fromParent ++ own
    }

    // The parent's contributions to the multiple key `name`, each with its entry in this scope.
    private def inheritedEntries(name: String): List[Entry[F]] =
      parent.toList.flatMap(_.contributions(name)).map(reg => inherited.getOrElseUpdate(reg, new Entry(reg)))

    private def entriesOf(name: String): List[Entry[F]] = inheritedEntries(name) ++ entries.getOrElse(name, Nil)

    private def contributions(name: String): List[Reg[F]] = entriesOf(name).map(_.reg)

    // ---- building and releasing ----

    private def discard(entry: Entry[F]): F[Unit] =
      delay {
        if (!entry.built) None
        else {
          val release = entry.release
          entry.built = false
          entry.value = null
          entry.release = None
          built -= entry
          release
        }
      }.flatMap {
        case Some(release) =>
          release.handleErrorWith { error =>
            shared.options.onReleaseError match {
              case Some(report) => report(entry.reg.key, error)
              case None         => delay(System.err.println(s"Releasing ${entry.reg.key} failed: $error"))
            }
          }
        case None => A.unit
      }

    private def build(entry: Entry[F]): F[Any] =
      if (entry.built) A.pure(entry.value)
      else
        for {
          deps          <- entry.reg.deps.traverse(resolve)
          allocated     <- entry.reg.service.make(deps).allocated
          (value, free) = allocated
          _             <- delay { entry.value = value; entry.built = true; entry.release = Some(free); built += entry }
          _             <- shared.options.onBuild.traverse_(_(entry.reg.key))
        } yield value

    private def resolve(key: Key[_]): F[Any] =
      delay {
        resolving.indexWhere(_.name == key.name) match {
          case -1 => resolving = key :: resolving
          case i =>
            val cycle = (resolving.take(i + 1).reverse :+ key).map(_.name).mkString(" -> ")
            throw DependencyError(s"Circular dependency: $cycle")
        }
      } >> A.guarantee(resolveNow(key), delay { resolving = resolving.drop(1) })

    private def resolveNow(key: Key[_]): F[Any] =
      if (key.isMultiple)
        lists.get(key.name) match {
          case Some(list) => A.pure(list)
          case None =>
            val list = new LiveList[F, Any]
            delay(lists.put(key.name, list)) >> refill(Some(key.name)).as(list)
        }
      else if (parent.isDefined && !entries.contains(key.name)) parent.get.resolve(key)
      else
        entries.get(key.name).flatMap(_.headOption) match {
          case Some(entry) => build(entry)
          case None =>
            val requiredBy = resolving.drop(1).headOption.fold("")(k => s" (required by $k)")
            A.raiseError(DependencyError(s"No service for $key$requiredBy"))
        }

    // Fills the live lists (only `only`, if given) with what can be built now, keeping their identity.
    private def refill(only: Option[String]): F[Unit] =
      lists.toList.filter { case (name, _) => only.forall(_ == name) }.traverse_ { case (name, list) =>
        entriesOf(name)
          .filter(e => e.built || e.reg.deps.forall(hasNow(_)))
          .traverse(build)
          .flatMap(values => delay(list.values = values))
      }

    // Releases what was built with `key`, and in cascade what was built with that. Dependents of a multiple key are
    // kept: their live list updates itself.
    private def invalidate(key: Key[_]): F[Unit] = {
      val dependents = (entries.values.flatten ++ inherited.values).toList.filter(e => e.built && e.reg.deps.exists(_.name == key.name))
      dependents.traverse_(e => (if (e.reg.key.isMultiple) A.unit else invalidate(e.reg.key)) >> discard(e))
    }

    private def pruneInherited: F[Unit] =
      inherited.toList.traverse_ { case (reg, entry) =>
        if (parent.exists(_.contributions(reg.key.name).exists(_ eq reg))) A.unit
        else discard(entry) >> delay(inherited.remove(reg)).void
      }

    // Reports the change to this container's listeners (after the lock) and to the scopes.
    private def notifyChange(key: Key[_]): F[Unit] =
      delay(shared.pending += ((listeners.toList, key))) >> children.toList.traverse_(_.parentChanged(key))

    private def parentChanged(key: Key[_]): F[Unit] =
      (if (key.isMultiple) A.unit else invalidate(key)) >> pruneInherited >> refill(None) >> notifyChange(key)

    // ---- registering ----

    private def removeEntry(entry: Entry[F]): F[Boolean] =
      delay {
        entries.get(entry.reg.key.name) match {
          case Some(list) if list.contains(entry) =>
            list -= entry
            if (list.isEmpty) entries.remove(entry.reg.key.name)
            true
          case _ => false
        }
      }.flatTap { removed =>
        if (!removed) A.unit
        // Dependents first: they may still use the service while they are released.
        else (if (entry.reg.key.isMultiple) A.unit else invalidate(entry.reg.key)) >> discard(entry) >> refill(None)
      }

    private def register(service: Service[F]): F[Entry[F]] = {
      val entry = new Entry(new Reg(service))
      val key = service.key
      delay {
        val list = entries.getOrElse(key.name, mutable.ArrayBuffer.empty[Entry[F]])
        if (list.headOption.exists(_.reg.key.isMultiple != key.isMultiple))
          throw DependencyError(s"$key is already registered with another cardinality")
        if (!key.isMultiple && list.nonEmpty) throw DependencyError(s"$key already has a service")
        list += entry
        entries.put(key.name, list)
      } >> refill(None).handleErrorWith(error => removeEntry(entry) >> A.raiseError(error)) >> notifyChange(key).as(entry)
    }

    private def unregister(done: List[Entry[F]]): F[Unit] =
      done.traverse_(e => removeEntry(e).flatMap(removed => if (removed) notifyChange(e.reg.key) else A.unit))

    private[Container] def disposeAll: F[Unit] =
      children.toList.traverse_(_.disposeAll) >>
        delay(built.toList.reverse).flatMap(_.traverse_(discard)) >>
        delay {
          entries.clear()
          inherited.clear()
          lists.clear()
          listeners.clear()
          children.clear()
          parent.foreach(_.children -= this)
        }

    // ---- public API ----

    def provide(services: Service[F]*): F[Container.Registration[F]] =
      shared.locked {
        def loop(rest: List[Service[F]], done: List[Entry[F]]): F[List[Entry[F]]] = rest match {
          case Nil => A.pure(done)
          case service :: tail =>
            register(service).attempt.flatMap {
              case Right(entry) => loop(tail, entry :: done)
              case Left(error)  => unregister(done) >> A.raiseError(error)
            }
        }
        loop(services.toList, Nil).map { done =>
          new Registration[F] { def remove: F[Unit] = shared.locked(unregister(done)) }
        }
      }

    def get[T](key: Key.Single[T]): F[T] = shared.locked(resolve(key)).map(_.asInstanceOf[T])

    def get[T](key: Key.Multiple[T]): F[Many[F, T]] = shared.locked(resolve(key)).map(_.asInstanceOf[Many[F, T]])

    def has(key: Key[_]): F[Boolean] = shared.locked(delay(hasNow(key)))

    def missing(key: Key[_]): F[List[Key[_]]] = shared.locked(delay(missingNow(key)))

    def registeredKeys: F[List[Key[_]]] = shared.locked(delay(registeredNow))

    def onChange(listener: Key[_] => F[Unit]): F[Container.Registration[F]] =
      shared.locked(delay(listeners += listener)).as(new Registration[F] {
        def remove: F[Unit] = shared.locked(delay(listeners -= listener).void)
      })

    def scope: Resource[F, Container[F]] =
      Resource.make(shared.locked(delay { val child = new Impl[F](shared, Some(this)); children += child; child }))(child =>
        shared.locked(child.disposeAll)
      )
  }
}
