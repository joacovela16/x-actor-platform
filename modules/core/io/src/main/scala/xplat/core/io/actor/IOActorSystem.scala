package xplat.core.io.actor

import cats.effect.Resource
import cats.effect.kernel.{Async, Deferred, Outcome, Poll, Ref}
import cats.effect.std.{Queue, Supervisor}
import cats.implicits._
import com.softwaremill.quicklens.ModifyPimp
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api._
import xplat.core.io.actor.IOActorSystem.Command.{ChildDown, OnMessage, OnSignal, Stop}

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

final case class IOActorSystem[F[_]](private val context: Context[F, Nothing])(implicit A: Async[F]) extends ActorSystem[F] {

  override def name: String = context.name

  override def spawn[T](name: String, mailbox: Mailbox)(behavior: Behavior[F, T]): F[ActorRef[F, T]] = {
    context.spawn(name, mailbox)(behavior)
  }

  override def schedule[T](f: => F[T], delay: FiniteDuration): F[Cancelable[F]] = {
    context.schedule(f, delay)
  }

  override def terminate(): F[Unit] = {
    context.stop()
  }

}

object IOActorSystem {

  def apply[F[_]](name: String)(implicit A: Async[F]): Resource[F, ActorSystem[F]] = {

    for {
      // Shutdown waits for actors to stop, but cancels pending scheduled tasks instead of waiting for their delay.
      supervisor <- Supervisor[F](await = true)
      timers <- Supervisor[F](await = false)
      as <- Resource.make(
        for {
          root <- A.uncancelable(poll => Actor[F, Nothing](
            name = name,
            behavior = Behaviors.same[F, Nothing],
            parentPath = Seq("xplat"),
            mailbox = Mailbox.Unbounded,
            notifyShutdown = (stopped: Deferred[F, Unit]) => stopped.complete(()).void,
            supervisor = supervisor,
            timers = timers,
            poll = poll
          ))
        } yield IOActorSystem(root._2)
      )(r => r.terminate())
    } yield as
  }

  /** Signals handled by every actor when its own handler does not handle them. */
  private def builtinSignals[F[_], T]: PartialFunction[Signal, Behavior[F, T]] = { case Signal.Kill =>
    Behavior.Stop[F, T]()
  }

  final case class Link[F[_]](name: String, requestShutdown: F[Unit])

  /** Children registry of an actor. Once `closed`, the actor is shutting down and no new child can be added. */
  final case class Children[F[_]](links: Map[String, Link[F]], closed: Boolean)

  sealed trait Command[F[_], T] {}

  object Command {
    final case class OnMessage[F[_], T](message: T) extends Command[F, T]
    final case class OnSignal[F[_], T](signal: Signal) extends Command[F, T]
    /** Sent by a child when it stops. The parent completes `released` once the child name is free. */
    final case class ChildDown[F[_], T](name: String, released: Deferred[F, Unit]) extends Command[F, T]
    final case class Stop[F[_], T]() extends Command[F, T]

    implicit def fromT[F[_], T](value: T): Command[F, T] = OnMessage(value)
    implicit def fromSignal[F[_], T](value: Signal): Command[F, T] = OnSignal(value)
  }

  sealed trait State[F[_], T] {
    def behavior: Behavior[F, T]
    def signalHandler: PartialFunction[Signal, Behavior[F, T]]
    def context: Context[F, T]
    def notifyShutdownDone: F[Unit]
    def children: Ref[F, Children[F]]
    def reduce(command: Command[F, T]): F[State[F, T]]

    def withBehavior(behavior: Behavior[F, T]): State[F, T]

    def withSignalHandler(sg: PartialFunction[Signal, Behavior[F, T]])(implicit A: Async[F]): State[F, T] = {
      this match {
        case item: State.Init[F, T] =>
          item.modify(_.signalHandler).setTo(sg)
        case item: State.Running[F, T] =>
          item.modify(_.signalHandler).setTo(sg)
        case item =>
          item
      }
    }

    def setup(): F[State[F, T]]

    override def toString: String = context.path
  }

  private object State {

    /** Resolves `behavior` until it is a message handler, `Same` or a stop. Setup behaviors (context, timers, stash) run
      * here, once, and are not kept as the current behavior.
      */
    private def fold[F[_], T](state: State[F, T], behavior: Behavior[F, T])(implicit A: Async[F]): F[State[F, T]] = {
      behavior match {
        case Behavior.Timer(time, message, f) =>
          handleError[F, Behavior[F, T], T](
            state.context.schedule(state.context.pipe(message), time).map(f),
            state,
            b => fold(state, b)
          )
        case Behavior.Stasher(size, f) =>
          handleError[F, Behavior[F, T], T](
            IOStash[F, T](state.context, size).map(f),
            state,
            b => fold(state, b)
          )
        case Behavior.Wrapper(effect) =>
          handleError[F, Behavior[F, T], T](
            effect,
            state,
            b => fold(state, b)
          )
        case Behavior.OnMessage(f) =>
          A.delay(state.withBehavior(Behavior.OnMessage(f)))
        case Behavior.OnContext(f) =>
          handleError[F, Behavior[F, T], T](
            A.delay(f(state.context)),
            state,
            b => fold(state, b)
          )
        case Behavior.Stop() =>
          terminate(state)
        case Behavior.OnSignal(f, nested) =>
          val next: State[F, T] = state.withSignalHandler(f)
          fold(next, nested)
        case _ =>
          A.delay(state)
      }
    }

    private def terminate[F[_], T](state: State[F, T])(implicit A: Async[F]): F[State[F, T]] =
      State.Terminated(state.behavior, state.context, state.notifyShutdownDone, state.children, state.signalHandler).setup()

    private def handleError[F[_], T, A](f: => F[T], state: State[F, A], onSuccess: T => F[State[F, A]])(implicit A: Async[F]): F[State[F, A]] = {

      A.defer(f).flatMap(onSuccess).recoverWith { error =>
        val signal: Signal = Signal.OnError(error)
        val isDefined: Boolean = state.signalHandler.isDefinedAt(signal)

        if (isDefined) {
          A
            .delay(state.signalHandler(signal))
            .flatMap { b =>
              fold(state, b)
            }
            .recoverWith { error =>
              state.context.log.error(error.getLocalizedMessage, error) >> terminate(state)
            }
        } else {
          state.context.log.error(error.getLocalizedMessage, error) >> terminate(state)
        }
      }
    }

    final case class Init[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        notifyShutdownDone: F[Unit],
        children: Ref[F, Children[F]],
        signalHandler: PartialFunction[Signal, Behavior[F, T]]
    )(implicit A: Async[F])
        extends State[F, T] {

      def setup(): F[State[F, T]] = {
        fold(this.withBehavior(Behavior.Same()), behavior).flatMap {
          case s: Init[F, T] => Running[F, T](s.behavior, s.context, s.notifyShutdownDone, s.children, s.signalHandler).setup()
          case stopped       => A.pure(stopped)
        }
      }

      override def reduce(command: Command[F, T]): F[State[F, T]] = A.pure(this)

      override def withBehavior(behavior: Behavior[F, T]): State[F, T] = this.modify(_.behavior).setTo(behavior)
    }

    private final case class Running[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        notifyShutdownDone: F[Unit],
        children: Ref[F, Children[F]],
        signalHandler: PartialFunction[Signal, Behavior[F, T]]
    )(implicit A: Async[F])
        extends State[F, T] {

      def setup(): F[State[F, T]] = A.pure(this)

      override def withBehavior(behavior: Behavior[F, T]): State[F, T] = this.modify(_.behavior).setTo(behavior)

      override def reduce(command: Command[F, T]): F[State[F, T]] = {
        command match {
          case OnMessage(message) =>
            onMessage(message, this, behavior)
          case OnSignal(signal) =>
            onSignal(signal)
          case Stop() =>
            terminate(this)
          case ChildDown(name, released) =>
            children.update(c => c.copy(links = c.links - name)) >> released.complete(()).as(this)
        }
      }

      private def onSignal(signal: Signal): F[State[F, T]] = {
        val handler = signalHandler.orElse(builtinSignals[F, T])
        if (handler.isDefinedAt(signal)) {
          handleError[F, Behavior[F, T], T](
            A.delay(handler(signal)),
            this,
            b => fold(this, b)
          )
        } else {
          A.pure(this)
        }
      }

      private def onMessage(message: T, state: State[F, T], behavior: Behavior[F, T]): F[State[F, T]] = {
        behavior match {
          case item: Behavior.OnMessage[F, T] =>
            handleError[F, Behavior[F, T], T](
              A.delay(item.func(message)),
              state,
              { next =>
                fold(state.withBehavior(behavior), next)
              }
            )
          case Behavior.OnContext(f) =>
            handleError[F, Behavior[F, T], T](
              A.delay(f(state.context)),
              state,
              b => onMessage(message, state, b)
            )
          case Behavior.OnSignal(f, nested) =>
            onMessage(message, state.withSignalHandler(f), nested)
          case Behavior.Stop() =>
            terminate(state)
          case Behavior.Wrapper(effect) =>
            handleError[F, Behavior[F, T], T](
              effect,
              state,
              b => onMessage(message, state, b)
            )
          case item: Behavior.Timer[F, T] =>
            fold(state, item) >>= (x => onMessage(message, x, x.behavior))
          case item: Behavior.Stasher[F, T] =>
            fold(state, item) >>= (x => onMessage(message, x, x.behavior))
          case _ =>
            A.pure(state)
        }
      }

    }

    private final case class Terminated[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        notifyShutdownDone: F[Unit],
        children: Ref[F, Children[F]],
        signalHandler: PartialFunction[Signal, Behavior[F, T]]
    )(implicit A: Async[F])
        extends State[F, T] {

      override def withBehavior(behavior: Behavior[F, T]): State[F, T] = this.modify(_.behavior).setTo(behavior)

      /** Closes the children registry and asks every child to stop. The actor finishes once the last child is down. */
      def setup(): F[State[F, T]] = {
        val stopped = this.withBehavior(Behavior.Stop())
        children.modify(c => (c.copy(closed = true), c.links)).flatMap { links =>
          if (links.isEmpty) {
            finish.as(stopped)
          } else {
            // Requests run on their own fibers so a child that is still being built never blocks this actor.
            context.log.debug(s"Starting shutting down actor ${context.path}") >>
              links.values.toList.traverse_(link => A.start(link.requestShutdown.attempt)).as(stopped)
          }
        }
      }

      def reduce(command: Command[F, T]): F[State[F, T]] = {
        command match {
          case ChildDown(name, released) =>
            children
              .modify { c =>
                val next = c.copy(links = c.links - name)
                (next, c.links.contains(name) && next.links.isEmpty)
              }
              .flatTap(_ => released.complete(()))
              .flatMap(lastChild => if (lastChild) finish.as(this) else A.pure(this))
          case _ =>
            A.pure(this)
        }
      }

      /** Delivers PostStop to the actor, then closes its mailbox. */
      private def finish: F[Unit] = postStop >> notifyShutdownDone

      private def postStop: F[Unit] = {
        if (signalHandler.isDefinedAt(Signal.PostStop)) {
          runEffects(A.delay(signalHandler(Signal.PostStop)))
            .handleErrorWith(error => context.log.error(error.getLocalizedMessage, error))
        } else {
          A.unit
        }
      }

      /** Runs the effects of a behavior returned while stopping. The behavior itself is discarded. */
      private def runEffects(fb: F[Behavior[F, T]]): F[Unit] = fb.flatMap {
        case Behavior.Wrapper(effect) => runEffects(effect)
        case Behavior.OnContext(f)    => runEffects(A.delay(f(context)))
        case _                        => A.unit
      }

    }

  }

  private final case class IOStash[F[_], T](private val queue: Queue[F, T], context: Context[F, T])(implicit A: Async[F]) extends Stash[F, T] {
    override def isEmpty: F[Boolean] = queue.size map (_ == 0)

    override def size(): F[Int] = queue.size

    override def push(message: T): F[Unit] = queue.tryOffer(message).void

    override def unstashAll(): F[Unit] = {
      queue.tryTakeN(None) >>= { xs =>
        xs.traverse_(x => context.pipe(x))
      }
    }

    override def unstashAll(b: => Behavior[F, T]): F[Behavior[F, T]] = unstashAll().as(b)
  }

  private object IOStash {

    def apply[F[_], T](context: Context[F, T], size: Int)(implicit A: Async[F]): F[Stash[F, T]] = {
      if (size == 0) {
        Queue.unbounded[F, T].map(q => IOStash(q, context))
      } else {
        Queue.bounded[F, T](size).map(q => IOStash(q, context))
      }
    }

  }

  private final case class IOContext[F[_], T](private val actor: Actor[F, T], log: Logger[F])(implicit A: Async[F]) extends Context[F, T] {
    override def name: String = actor.name
    override def path: String = actor.path
    override def self: ActorRef[F, T] = actor
    override def spawn[A](name: String, mailbox: Mailbox)(b: Behavior[F, A]): F[ActorRef[F, A]] = actor.spawn[A](name, mailbox, b)
    override def pipe(msg: T): F[Unit] = actor.pipe(msg)
    override def signal(signal: Signal): F[Unit] = actor.signal(signal)
    override def stopChild(name: String): F[Unit] = actor.stopChild(name)
    override def stop(): F[Unit] = actor.stop()

    override def schedule[A](f: => F[A], delay: FiniteDuration): F[Cancelable[F]] = {
      actor.timers.supervise(A.sleep(delay) >> f).map(fiber => () => fiber.cancel)
    }

  }

  final case class Actor[F[_], T](
      name: String,
      parentPath: Seq[String],
      mailbox: IOMailbox[F, Command[F, T]],
      supervisor: Supervisor[F],
      timers: Supervisor[F],
      children: Ref[F, Children[F]],
      stopped: Deferred[F, Unit],
      log: Logger[F]
  )(implicit A: Async[F])
      extends ActorRef[F, T] {
    self =>
    private lazy val _path = s"${parentPath.mkString("/")}/$name"

    /** Enqueues a command outside the mailbox limit. Used for signals, stops and messages the actor sends to itself. */
    private def push(cmd: Command[F, T]): F[Unit] = {
      mailbox.offerSystem(cmd).flatMap { accepted =>
        if (accepted) A.unit else A.raiseError[Unit](new Exception(s"Actor $path already stopped"))
      }
    }

    def pipe(msg: T): F[Unit] = push(Command.OnMessage(msg))

    def stopChild(name: String): F[Unit] = {
      children.get.flatMap(_.links.get(name).traverse_(_.requestShutdown))
    }

    def signal(signal: Signal): F[Unit] = {
      push(Command.OnSignal(signal))
    }

    def stop(): F[Unit] = push(Command.Stop())

    /** Builds the child on the calling fiber, so it also works from inside this actor's own handlers. The name is
      * reserved first, which rejects duplicates and children of an actor that is shutting down.
      *
      * Only the setup of the child's behavior can be cancelled. Whatever ends the build, `built` is completed, because
      * stopping this actor waits on it to stop the child.
      */
    def spawn[A](childName: String, childMailbox: Mailbox, b: Behavior[F, A]): F[ActorRef[F, A]] = {
      Deferred[F, Option[Actor[F, A]]].flatMap { built =>
        val link = Link[F](childName, built.get.flatMap(_.traverse_(_.stop())))

        val reserve: F[Unit] = children.modify { c =>
          if (c.closed) (c, A.raiseError[Unit](new Exception(s"Actor $path is down")))
          else if (c.links.contains(childName)) (c, A.raiseError[Unit](new Exception(s"children $childName exists")))
          else (c.copy(links = c.links + (childName -> link)), A.unit)
        }.flatten

        def build(poll: Poll[F]): F[ActorRef[F, A]] = A.guaranteeCase(
          A.defer(
            Actor[F, A](
              name = childName,
              behavior = b,
              parentPath = parentPath :+ self.name,
              mailbox = childMailbox,
              notifyShutdown = (released: Deferred[F, Unit]) => push(Command.ChildDown(childName, released)).handleErrorWith(_ => released.complete(()).void),
              supervisor = supervisor,
              timers = timers,
              poll = poll
            )
          )
            .flatMap { case (child, _) => built.complete(Some(child)).as(child: ActorRef[F, A]) }
        ) {
          case Outcome.Succeeded(_) => A.unit
          case _                    => built.complete(None) >> Deferred[F, Unit].flatMap(released => push(Command.ChildDown(childName, released))).attempt.void
        }

        A.uncancelable(poll => reserve >> build(poll))
      }
    }

    override def path: String = _path

    override def terminated: F[Unit] = stopped.get

    override def ask[A](f: ReplyTo[A] => T, timeout: FiniteDuration): F[A] = IOAsk(send)(f, timeout)

    override def send(msg: T): F[Unit] = {
      mailbox.send(Command.OnMessage(msg), path, log)
    }

  }

  private object Actor {

    def apply[F[_], T](
        name: String,
        behavior: Behavior[F, T],
        parentPath: Seq[String],
        mailbox: Mailbox,
        notifyShutdown: Deferred[F, Unit] => F[Unit],
        supervisor: Supervisor[F],
        timers: Supervisor[F],
        poll: Poll[F]
    )(implicit A: Async[F]): F[(Actor[F, T], Context[F, T])] = {

      for {
        box <- IOMailbox[F, Command[F, T]](mailbox)
        children <- Ref.of[F, Children[F]](Children(Map.empty, closed = false))
        stopped <- Deferred[F, Unit]
        logLib <- Slf4jLogger.fromName[F](s"${parentPath.mkString("/")}/$name")
        log = IOLogger.build(logLib)
        actor = Actor[F, T](name, parentPath, box, supervisor, timers, children, stopped, log)
        context = IOContext[F, T](actor, log)
        _ <- log.debug(s"Creating actor ${actor.path}")
        // Only the setup runs user code that may take long, so it is the only cancelable step.
        initState <- poll(State
          .Init[F, T](
            behavior = behavior,
            context = context,
            notifyShutdownDone = box.close,
            children = children,
            signalHandler = PartialFunction.empty
          )
          .setup())

        _ <- supervisor.supervise(
          box.stream
            .evalScan[F, State[F, T]](initState)(_ reduce _)
            .onFinalize(context.log.debug(s"Actor ${context.path} is down") >> notifyShutdown(stopped))
            .compile
            .drain
        )

      } yield (actor, context)
    }

  }

  private[actor] final case class IOLogger[F[_]](private val log: Log4Cats[F]) extends Logger[F] {
    def info(str: String): F[Unit] = log.info(str)
    def info(str: String, throwable: Throwable): F[Unit] = log.info(throwable)(str)

    def warn(str: String): F[Unit] = log.warn(str)
    def warn(str: String, throwable: Throwable): F[Unit] = log.warn(throwable)(str)

    def error(str: String): F[Unit] = log.error(str)
    def error(str: String, throwable: Throwable): F[Unit] = log.error(throwable)(str)

    def debug(str: String): F[Unit] = log.debug(str)
    def debug(str: String, throwable: Throwable): F[Unit] = log.debug(throwable)(str)
  }

  private[actor] object IOLogger {
    def build[F[_]](log: Log4Cats[F]): Logger[F] = IOLogger[F](log)
  }

}
