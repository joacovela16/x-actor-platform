package xplat.core.io.actor

import cats.effect.Resource
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.{Queue, Supervisor}
import cats.implicits._
import com.softwaremill.quicklens.ModifyPimp
import fs2.concurrent.Channel
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.Behavior.{OnContext, Same, Wrapper}
import xplat.core.api._
import xplat.core.io.actor.IOActorSystem.Command.{AddChild, ChildDown, OnMessage, OnSignal, Stop, StopChild}

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

final case class IOActorSystem[F[_]](private val context: Context[F, Nothing])(implicit A: Async[F]) extends ActorSystem[F] {

  override def spawn[T](name: String)(builder: Behaviors[F, T] => Behavior[F, T]): F[ActorRef[F, T]] = {
    context.spawn(name)(builder)
  }

  override def schedule[T](f: => F[T], delay: FiniteDuration): F[Cancelable[F]] = {
    A.start(A.sleep(delay) >> f).map(f => () => f.cancel)
  }

  override def terminate(): F[Unit] = {
    context.stop()
  }

}

object IOActorSystem {

  def apply[F[_]](name: String)(implicit A: Async[F]): Resource[F, ActorSystem[F]] = {

    for {
      supervisor <- Supervisor[F](await = true)
      as <- Resource.make(
        for {
          root <- Actor[F, Nothing](
            name = name,
            behavior = IOBehaviors[F, Nothing]().same,
            parentPath = Seq("xplat"),
            notifyShutdown = A.unit,
            supervisor = supervisor
          )
        } yield IOActorSystem(root._2)
      )(r => r.terminate())
    } yield as
  }

  private def defaultHandler[F[_], T]: PartialFunction[Signal, Behavior[F, T]] = { case _ =>
    Behavior.Stop[F, T]()
  }

  final case class Link[F[_]](name: String, requestShutdown: F[Unit])

  sealed trait Command[F[_], T] {}

  object Command {
    final case class OnMessage[F[_], T](message: T) extends Command[F, T]
    final case class OnSignal[F[_], T](signal: Signal) extends Command[F, T]
    final case class StopChild[F[_], T](name: String) extends Command[F, T]
    final case class ChildDown[F[_], T](name: String) extends Command[F, T]
    final case class Stop[F[_], T]() extends Command[F, T]
    final case class AddChild[F[_], T](name: String, onBuild: F[Link[F]], onError: Deferred[F, Throwable]) extends Command[F, T]

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
          A.delay(state.withBehavior(Behavior.OnMessage(f)))
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
          A.delay(state)
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

          for {
            _ <- state.context.log.error(error.getLocalizedMessage, error)
            next <- State.Terminated(state.behavior, state.context, state.notifyShutdownDone, state.children, state.signalHandler).setup()
          } yield next
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
          notifyShutdownDone.as(this.withBehavior(Behavior.Stop()))
        } else {
          context.log.info(s"Starting shuting down actor ${context.path}") >>
            children.values
              .map(x => x.requestShutdown.as[Option[String]](None).handleError(_ => Option(x.name)))
              .toList
              .sequence
              .map { xs =>
                val nextChildren: Map[String, Link[F]] = xs.flatten.foldLeft(children) { case (acc, item) => acc - item }
                this.modify(_.children).setTo(nextChildren).withBehavior(Behavior.Stop())
              }
        }
      }

      def reduce(command: Command[F, T]): F[State[F, T]] = {
        command match {
          case ChildDown(name) =>
            val nextChildren: Map[String, Link[F]] = children - name
            if (nextChildren.isEmpty) {
              notifyShutdownDone.as(this.modify(_.children).setTo(nextChildren))
            } else {
              A.pure(this.modify(_.children).setTo(nextChildren))
            }
          case other =>
            A.pure(this)
        }
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

    def apply[F[_]: Async, T](context: Context[F, T], size: Int): F[Stash[F, T]] = {
      if (size == 0) {
        Queue.unbounded[F, T].map(q => IOStash(q, context))
      } else {
        Queue.bounded[F, T](size).map(q => IOStash(q, context))
      }
    }

  }

  private final case class IOBehaviors[F[_], T]()(implicit A: Async[F]) extends Behaviors[F, T] {

    override def stash(size: Int)(f: Stash[F, T] => Behavior[F, T]): Behavior[F, T] = {
      context(c => IOStash[F, T](c, size) map (x => f(x)))
    }

    override def timer(time: FiniteDuration, message: T)(f: Cancelable[F] => Behavior[F, T]): Behavior[F, T] = {
      context(c => c.schedule(c.pipe(message), time) map f)
    }

  }

  final case class IOContext[F[_], T](private val actor: Actor[F, T], log: Logger[F])(implicit A: Async[F]) extends Context[F, T] {
    override def name: String = actor.name
    override def path: String = actor.path
    override def self: ActorRef[F, T] = actor
    override def spawn[A](name: String)(b: Behaviors[F, A] => Behavior[F, A]): F[ActorRef[F, A]] = actor.spawn(name, b)
    override def pipe(msg: T): F[Unit] = actor.send(msg)
    override def signal(signal: Signal): F[Unit] = actor.signal(signal)
    override def stopChild(name: String): F[Unit] = actor.stopChild(name)
    override def stop(): F[Unit] = actor.stop()

    override def schedule[A](f: => F[A], delay: FiniteDuration): F[Cancelable[F]] = {
      A.start(A.sleep(delay) >> f).map(f => () => f.cancel)
    }

  }

  final case class Actor[F[_], T](
      name: String,
      parentPath: Seq[String],
      channel: Channel[F, Command[F, T]],
      supervisor: Supervisor[F]
  )(implicit A: Async[F])
      extends ActorRef[F, T] {
    self =>
    private lazy val _path = s"${parentPath.mkString("/")}/$name"

    private def push(cmd: Command[F, T]): F[Unit] = {
      channel.send(cmd).flatMap {
        case Left(_) => A.raiseError[Unit](new Exception(s"Actor $path already stopped"))
        case _       => A.unit
      }
    }

    def stopChild(name: String): F[Unit] = {
      push(Command.StopChild(name))
    }

    def signal(signal: Signal): F[Unit] = {
      push(Command.OnSignal(signal))
    }

    def stop(): F[Unit] = push(Command.Stop())

    def spawn[A](name: String, b: Behaviors[F, A] => Behavior[F, A]): F[ActorRef[F, A]] = {
      Async[F].async { resolve =>
        for {
          onBuild <- Deferred[F, Actor[F, A]]
          onError <- Deferred[F, Throwable]
          _ <- push(
            Command.AddChild(
              name,
              Actor[F, A](
                name = name,
                behavior = b(IOBehaviors()),
                parentPath = parentPath :+ self.name,
                notifyShutdown = push(Command.ChildDown(name)),
                supervisor = supervisor
              ) >>= { case (actorRef, _) =>
                onBuild
                  .complete(actorRef)
                  .as(
                    Link[F](
                      name = name,
                      requestShutdown = actorRef.stop()
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

    override def ask[A](f: ActorRef[F, A] => T, timeout: FiniteDuration): F[A] = {
      for {
        onMessage <- Deferred[F, A]
        ref <- spawn[A](
          UUID.randomUUID().toString,
          b => {
            b.message { m =>
              onMessage.complete(m).as(b.stop)
            }
          }
        )
        _ <- send(f(ref))
        result <- A
          .race(A.sleep(timeout) >> A.pure(new Exception("Ask timeout")), onMessage.get)
          .flatMap {
            case Left(value) =>
              A.raiseError[A](value)
            case Right(value) =>
              A.pure(value)
          }
      } yield result
    }

    override def send(msg: T): F[Unit] = {
      push(msg)
    }

  }

  private object Actor {

    def apply[F[_], T](
        name: String,
        behavior: Behavior[F, T],
        parentPath: Seq[String],
        notifyShutdown: F[Unit],
        supervisor: Supervisor[F]
    )(implicit A: Async[F]): F[(Actor[F, T], Context[F, T])] = {

      for {

        channel <- Channel.synchronous[F, Command[F, T]]
        actor = Actor[F, T](name, parentPath, channel, supervisor)
        logLib <- Slf4jLogger.fromName[F](actor.path)
        log = IOLogger.build(logLib)
        context = IOContext[F, T](actor, log)
        _ <- log.info(s"Creating actor ${actor.path}")
        initState <- State
          .Init[F, T](
            behavior = behavior,
            context = context,
            notifyShutdownDone = channel.close.void,
            children = Map.empty,
            signalHandler = defaultHandler
          )
          .setup()

        _ <- supervisor.supervise(
          channel.stream
            .evalScan[F, State[F, T]](initState)(_ reduce _)
            .onFinalize(context.log.info(s"Actor ${context.path} is down") >> notifyShutdown)
            .compile
            .drain
        )

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
