package xplat.core.io.actor

import cats.effect.implicits._
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Queue
import cats.implicits._
import com.softwaremill.quicklens.ModifyPimp
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.Behavior.{OnContext, Same, Wrapper}
import xplat.core.api._
import xplat.core.io.actor.IOActorSystem.Command.{AddChild, ChildDown, OnMessage, OnSignal, Stop, StopChild}

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

final case class IOActorSystem[F[_]](private val context: Context[F, Nothing], onShutdownAll: Deferred[F, Unit])(implicit A: Async[F]) extends ActorSystem[F] {

  override def spawn[T](name: String)(builder: Behaviors[F, T] => Behavior[F, T]): F[ActorRef[F, T]] = {
    context.spawn(name)(builder)
  }

  override def schedule[T](f: => F[T], delay: FiniteDuration): F[Cancelable[F]] = {
    A.start(A.sleep(delay) >> f).map(f => () => f.cancel)
  }

  override def terminate(): F[Unit] = {
    for {
      _ <- context.stop()
      _ <- onShutdownAll.get
    } yield ()
  }

}

object IOActorSystem {
  implicit def mkSystem[F[_]: Async]: F[ActorSystem[F]] = IOActorSystem[F]("root")

  def apply[F[_]](name: String)(implicit A: Async[F]): F[ActorSystem[F]] = {
    for {
      d <- Deferred[F, Unit]
      root <- Actor[F, Nothing](name, Behaviors[F, Nothing]().same, Seq("xplat"), d.complete(()).void)
    } yield IOActorSystem(root._2, d)
  }

  private def defaultHandler[F[_], T]: PartialFunction[Signal, Behavior[F, T]] = { case _ =>
    Behavior.Stop[F, T]()
  }

  final case class Link[F[_]](requestShutdown: F[Unit])

  sealed trait Command[F[_], T] {}

  object Command {
    final case class OnMessage[F[_], T](message: T) extends Command[F, T]
    final case class OnSignal[F[_], T](signal: Signal) extends Command[F, T]
    final case class StopChild[F[_], T](name: String) extends Command[F, T]
    final case class ChildDown[F[_], T](name: String) extends Command[F, T]
    final case class Stop[F[_], T]() extends Command[F, T]

    final case class AddChild[F[_], T](
        name: String,
        onBuild: F[Link[F]],
        onError: Deferred[F, Throwable]
    ) extends Command[F, T]

    implicit def fromT[F[_], T](value: T): Command[F, T] = OnMessage(value)
    implicit def fromSignal[F[_], T](value: Signal): Command[F, T] = OnSignal(value)
  }

  sealed trait State[F[_], T] {
    def behavior: Behavior[F, T]
    def signalHandler: PartialFunction[Signal, Behavior[F, T]]
    def context: Context[F, T]
    def notifyShutdownDone: F[Unit]
    def children: Map[String, Link[F]]
    def reduce(command: Command[F, T]): F[State[F, T]]

    def withBehavior(behavior: Behavior[F, T])(implicit A: Async[F]): State[F, T] = {
      this match {
        case item: State.Init[F, T] =>
          item.modify(_.behavior).setTo(behavior)
        case item: State.Running[F, T] =>
          item.modify(_.behavior).setTo(behavior)
        case item: State.Terminated[F, T] =>
          item.modify(_.behavior).setTo(behavior)
      }
    }

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

    private def fold[F[_], T](state: State[F, T], behavior: Behavior[F, T])(implicit A: Async[F]): F[State[F, T]] = {
      behavior match {
        case Behavior.Wrapper(effect) =>
          handleError[F, Behavior[F, T], T](
            effect,
            state,
            b => fold(state, b)
          )
        case Behavior.OnMessage(f) =>
          A.pure(state.withBehavior(Behavior.OnMessage(f)))
        case Behavior.OnContext(f) =>
          handleError[F, Behavior[F, T], T](
            A.delay(f(state.context)),
            state,
            b => fold(state.withBehavior(behavior), b)
          )
        case Behavior.Stop() =>
          State.Terminated(behavior, state.context, state.notifyShutdownDone, state.children, state.signalHandler).setup()
        case Behavior.OnSignal(f, nested) =>
          val next: State[F, T] = state.withSignalHandler(f)
          fold(next, nested)
        case _ =>
          A.pure(state)
      }
    }

    private def handleError[F[_], T, A](f: => F[T], state: State[F, A], onSuccess: T => F[State[F, A]])(implicit A: Async[F]): F[State[F, A]] = {

      f.flatMap(onSuccess).recoverWith { error =>
        val signal: Signal = Signal.OnError(error)
        val isDefined: Boolean = state.signalHandler.isDefinedAt(signal)

        if (isDefined) {
          A
            .delay(state.signalHandler(signal))
            .flatMap { b =>
              fold(state, b)
            }
            .recoverWith { error =>
              state.context.log.error(error.getLocalizedMessage, error) >>
                State.Terminated(state.behavior, state.context, state.notifyShutdownDone, state.children, state.signalHandler).setup()
            }
        } else {
          State.Terminated(state.behavior, state.context, state.notifyShutdownDone, state.children, state.signalHandler).setup()
        }
      }
    }

    final case class Init[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        notifyShutdownDone: F[Unit],
        children: Map[String, Link[F]],
        signalHandler: PartialFunction[Signal, Behavior[F, T]]
    )(implicit A: Async[F])
        extends State[F, T] {

      def setup(): F[State[F, T]] = {
        fold(this, behavior).flatMap(s => Running[F, T](s.behavior, s.context, s.notifyShutdownDone, s.children, s.signalHandler).setup())
      }

      override def reduce(command: Command[F, T]): F[State[F, T]] = A.pure(this)
    }

    private final case class Running[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        notifyShutdownDone: F[Unit],
        children: Map[String, Link[F]],
        signalHandler: PartialFunction[Signal, Behavior[F, T]]
    )(implicit A: Async[F])
        extends State[F, T] {

      def setup(): F[State[F, T]] = A.pure(this)

      override def reduce(command: Command[F, T]): F[State[F, T]] = {
        command match {
          case OnMessage(message) =>
            onMessage(message, this, behavior)
          case OnSignal(signal) =>
            onSignal(signal, this, behavior)
          case StopChild(name) =>
            children.get(name) match {
              case None =>
                A.pure(this)
              case Some(link) =>
                link.requestShutdown.as(this)
            }
          case Stop() =>
            Terminated(behavior, context, notifyShutdownDone, children, signalHandler).setup()
          case AddChild(name, onBuild, onError) =>
            if (children.contains(name)) {
              onError.complete(new Exception(s"children $name exists")).as(this)
            } else {
              onBuild.map { link =>
                this.modify(_.children).using(xs => xs + (name -> link))
              }
            }
          case ChildDown(name) =>
            A.pure(this.modify(_.children).using(_ - name))
        }
      }

      private def onSignal(signal: Signal, state: State[F, T], behavior: Behavior[F, T]): F[State[F, T]] = {
        behavior match {
          case OnContext(f) =>
            handleError[F, Behavior[F, T], T](
              A.delay(f(state.context)),
              state,
              b => onSignal(signal, state, b)
            )
          case Behavior.Stop() =>
            Terminated(behavior, context, notifyShutdownDone, children, signalHandler).setup()
          case Same() =>
            state.pure[F]
          case Wrapper(effect) =>
            handleError[F, Behavior[F, T], T](
              effect,
              state,
              b => onSignal(signal, state, b)
            )
          case Behavior.OnMessage(f) =>
            state.withBehavior(Behavior.OnMessage(f)).pure[F]
          case Behavior.OnSignal(f, nested) =>
            if (f.isDefinedAt(signal)) {
              handleError[F, Behavior[F, T], T](
                A.delay(f(signal)),
                state,
                b => fold(state.withSignalHandler(f), b)
              )
            } else {
              fold(state.withSignalHandler(f), nested)
            }
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
            Terminated(behavior, context, notifyShutdownDone, children, signalHandler).setup()
          case Behavior.Wrapper(effect) =>
            handleError[F, Behavior[F, T], T](
              effect,
              state,
              b => onMessage(message, state, b)
            )
          case _ =>
            A.pure(state)
        }
      }

    }

    private final case class Terminated[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        notifyShutdownDone: F[Unit],
        children: Map[String, Link[F]],
        signalHandler: PartialFunction[Signal, Behavior[F, T]]
    )(implicit A: Async[F])
        extends State[F, T] {

      def setup(): F[State[F, T]] = {

        if (children.isEmpty) {
          notifyShutdownDone.start.as(this.withBehavior(Behavior.Stop())) <* context.log.info(s"Actor ${context.path} is down")
        } else {
          context.log.info(s"Starting shuting down actor ${context.path}") >>
            children.values.map(x => x.requestShutdown).toList.sequence_.start.as(this.withBehavior(Behavior.Stop()))
        }
      }

      def reduce(command: Command[F, T]): F[State[F, T]] = {
        command match {
          case ChildDown(name) =>
            val next: State[F, T] = this.modify(_.children).using(_ - name)
            val lastTask = if (next.children.isEmpty) {
              notifyShutdownDone.start.as(next) <* context.log.info(s"Actor ${context.path} is down")
            } else {
              next.pure[F]
            }

            lastTask <* context.log.info(s"Child down $name")
          case other =>
            A.pure(this)
        }
      }

    }

  }

  final case class IOContext[F[_], T](private val actor: Actor[F, T], log: Logger[F])(implicit A: Async[F]) extends Context[F, T] {
    override def name: String = actor.name
    override def path: String = actor.path
    override def self: ActorRef[F, T] = actor

    override def spawn[A](name: String)(b: Behaviors[F, A] => Behavior[F, A]): F[ActorRef[F, A]] = {
      actor.spawn(name, b(Behaviors()))
    }

    override def pipe(msg: T): F[Unit] = actor.push(msg)
    override def signal(signal: Signal): F[Unit] = actor.push(signal)
    override def stopChild(name: String): F[Unit] = actor.push(Command.StopChild(name))

    override def stop(): F[Unit] = actor.stop()
  }

  final case class Actor[F[_], T](
      name: String,
      parentPath: Seq[String],
      queue: Queue[F, Command[F, T]],
      isDown: F[Boolean]
  )(implicit A: Async[F])
      extends ActorRef[F, T] {
    self =>
    private lazy val _path = s"${parentPath.mkString("/")}/$name"

    def push(cmd: Command[F, T]): F[Unit] = {
      isDown >>= { down =>
        if (down) A.unit
        else queue.offer(cmd)//.start.void
      }
    }

    def stop(): F[Unit] = push(Command.Stop())

    def spawn[A](name: String, b: Behavior[F, A]): F[ActorRef[F, A]] = {
      Async[F].async { resolve =>
        for {
          onBuild <- Deferred[F, Actor[F, A]]
          onError <- Deferred[F, Throwable]
          _ <- push(
            Command.AddChild(
              name,
              Actor[F, A](
                name,
                b,
                parentPath :+ self.name,
                push(Command.ChildDown(name))
              ) >>= { case (actorRef, _) =>
                onBuild
                  .complete(actorRef)
                  .as(
                    Link[F](
                      requestShutdown = actorRef.push(Command.Stop())
                    )
                  )
              },
              onError
            )
          )
          result <- A.race(onError.get, onBuild.get)
        } yield {
          resolve(result)
          None
        }
      }
    }

    override def path: String = _path

    override def ask[A](f: ActorRef[F, A] => T, timeout: FiniteDuration): F[A] = ???

    override def send(msg: T): F[Unit] = push(msg)
  }

  object Actor {

    def apply[F[_], T](
        name: String,
        behavior: Behavior[F, T],
        parentPath: Seq[String],
        notifyShutdown: F[Unit]
    )(implicit A: Async[F]): F[(Actor[F, T], Context[F, T])] = {

      for {
        queue <- Queue.bounded[F, Command[F, T]](10)
        shutdownSignal <- SignallingRef[F].of(false)
        actor = Actor[F, T](name, parentPath, queue, shutdownSignal.get)
        logLib <- Slf4jLogger.fromName[F](actor.path)
        log = IOLogger.build(logLib)
        context = IOContext[F, T](actor, log)
        _ <- log.info(s"Creating actor ${actor.path}")
        initState <- State
          .Init[F, T](
            behavior,
            context,
            shutdownSignal.set(true) >> log.info(s"Calling down ${context.path}")  >> notifyShutdown,
            Map.empty,
            defaultHandler
          )
          .setup()
        _ <- Stream
          .fromQueueUnterminated(queue, 10)
          .interruptWhen(shutdownSignal)
          .evalScan[F, State[F, T]](initState)(_ reduce _)
          .compile
          .drain
          .start

      } yield (actor, context)
    }

  }

  private final case class IOLogger[F[_]](private val log: Log4Cats[F]) extends Logger[F] {
    def info(str: String): F[Unit] = log.info(str)
    def info(str: String, throwable: Throwable): F[Unit] = log.info(throwable)(str)

    def warn(str: String): F[Unit] = log.warn(str)
    def warn(str: String, throwable: Throwable): F[Unit] = log.warn(throwable)(str)

    def error(str: String): F[Unit] = log.error(str)
    def error(str: String, throwable: Throwable): F[Unit] = log.error(throwable)(str)

    def debug(str: String): F[Unit] = log.debug(str)
    def debug(str: String, throwable: Throwable): F[Unit] = log.debug(throwable)(str)
  }

  private object IOLogger {
    def build[F[_]](log: Log4Cats[F]): Logger[F] = IOLogger[F](log)
  }

}
