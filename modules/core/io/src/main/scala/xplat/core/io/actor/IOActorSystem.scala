package xplat.core.io.actor

import cats.Monad
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.{Queue, Supervisor}
import cats.implicits._
import com.softwaremill.quicklens.ModifyPimp
import fs2.Stream
import fs2.concurrent.SignallingRef
import xplat.core.api._

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions
import xplat.core.io.actor.IOActorSystem.Command.OnMessage
import xplat.core.io.actor.IOActorSystem.Command.OnSignal
import xplat.core.io.actor.IOActorSystem.Command.StopChild
import xplat.core.io.actor.IOActorSystem.Command.Stop
import xplat.core.io.actor.IOActorSystem.Command.AddChild

final case class IOActorSystem[F[_]]()(implicit A: Async[F]) extends ActorSystem[F] {
  override def spawn[T](name: String)(builder: Behaviors[F, T] => Behavior[F, T]): F[ActorRef[F, T]] = ???

  override def schedule[T](f: => F[T], delay: FiniteDuration): F[Cancelable[F]] = ???

  override def terminate(): F[Unit] = ???
}

object IOActorSystem {
  case object NotUsed

  final case class SignalHold[F[_], T](
      handler: PartialFunction[Signal, Behavior[F, T]],
      scope: Behavior[F, T]
  )

  final case class Link[F[_]]()

  sealed trait Command[F[_], T] {}

  object Command {
    final case class OnMessage[F[_], T](message: T) extends Command[F, T]
    final case class OnSignal[F[_], T](signal: Signal) extends Command[F, T]
    final case class StopChild[F[_], T](name: String) extends Command[F, T]
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
    def context: Context[F, T]
    def shutdownSignal: F[Unit]
    def children: Map[String, Link[F]]
    def reduce(command: Command[F, T]): F[State[F, T]]
    def withBehavior(behavior: Behavior[F, T]): State[F, T]
    def setup(): F[State[F, T]]
  }

  object State {

    private def fold[F[_], T](state: State[F, T], behavior: Behavior[F, T])(implicit A: Monad[F]): F[State[F, T]] = {
      behavior match {
        case Behavior.Wrapper(effect) =>
          effect.flatMap(b => fold(state, b))
        case Behavior.OnMessage(f) =>
          A.pure(state.withBehavior(Behavior.OnMessage(f)))
        case Behavior.OnContext(f) =>
          val next: Behavior[F, T] = f(state.context)
          fold(state.withBehavior(behavior), next)
        case _ =>
          A.pure(state)
      }
    }

    final case class Init[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        shutdownSignal: F[Unit],
        children: Map[String, Link[F]]
    )(implicit A: Monad[F])
        extends State[F, T] {

      def withBehavior(b: Behavior[F, T]): State[F, T] = this.modify(_.behavior).setTo(b)

      def setup(): F[State[F, T]] = {
        fold(this, behavior) map (s => Running(s.behavior, s.context, s.shutdownSignal, s.children))
      }

      override def reduce(command: Command[F, T]): F[State[F, T]] = A.pure(this)
    }

    final case class Running[F[_], T](
        behavior: Behavior[F, T],
        context: Context[F, T],
        shutdownSignal: F[Unit],
        children: Map[String, Link[F]]
    )(implicit A: Monad[F])
        extends State[F, T] {
      def withBehavior(b: Behavior[F, T]): State[F, T] = this.modify(_.behavior).setTo(b)

      def setup(): F[State[F, T]] = A.pure(this)

      override def reduce(command: Command[F, T]): F[State[F, T]] = {
        command match {
          case OnMessage(message) =>
            onMessage(message, this, behavior)
          case OnSignal(signal) =>
            ???
          case StopChild(name) =>
            ???
          case Stop() =>
            ???
          case AddChild(name, onBuild, onError) =>
            ???
        }
      }

      private def onMessage(message: T, state: State[F, T], behavior: Behavior[F, T]): F[State[F, T]] = {
        behavior match {
          case Behavior.OnMessage(f) =>
            val next: Behavior[F, T] = f(message)
            fold(state.withBehavior(behavior), next)
          case Behavior.OnContext(f) =>
            val next: Behavior[F, T] = f(context)
            onMessage(message, state, next)
          case Behavior.OnSignal(f, nested) =>
            ???
          case Behavior.Stop() =>
            ???
          case Behavior.Wrapper(effect) =>
            effect.flatMap(b => onMessage(message, state, b))
          case _ =>
            A.pure(state)
        }
      }

    }

  }

  /*
  final case class State[F[_], T](
      behavior: Behavior[F, T],
      context: Context[F, T],
      shutdownSignal: F[Unit],
      children: Map[String, Link[F]]
  )(implicit A: Monad[F]) {
    def widthBehavior(b: Behavior[F, T]): State[F, T] = this.modify(_.behavior).setTo(b)

    private def onMessage(message: T, state: State[F, T], behavior: Behavior[F, T]): F[State[F, T]] = {
      behavior match {
        case Behavior.OnMessage(f) =>
          val next: Behavior[F, T] = f(message)
          fold(state.widthBehavior(behavior), next)
        case Behavior.OnContext(f) =>
          val next: Behavior[F, T] = f(context)
          onMessage(message, state, next)
        case Behavior.OnSignal(f, nested) =>
          ???
        case Behavior.Stop() =>
          ???
        case Behavior.Wrapper(effect) =>
          effect.flatMap(b => onMessage(message, state, b))
        case _ =>
          A.pure(state)
      }
    }

    private def running(command: Command[F, T], state: State[F, T]): F[State[F, T]] = {
      command match {
        case Command.OnMessage(message) =>
          onMessage(message, state, state.behavior)
        case Command.OnSignal(signal)                           => ???
        case Command.StopChild(name)                            => ???
        case Command.AddChild(name, behavior, onBuild, onError) => ???
        case Command.Stop()                                     => ???
      }

    }

    def reduce(command: Command[F, T]): F[State[F, T]] = running(command, this)

  }
   */

  final case class IOContext[F[_], T](private val actor: Actor[F, T])(implicit A: Async[F]) extends Context[F, T] {
    override def name: String = actor.name
    override def self: ActorRef[F, T] = actor

    override def spawn[A](name: String)(b: Behaviors[F, A] => Behavior[F, A]): F[ActorRef[F, A]] = {
      Async[F].async { resolve =>
        for {
          onBuild <- Deferred[F, Actor[F, A]]
          onError <- Deferred[F, Throwable]
          _ <- actor.push(
            Command.AddChild(
              name,
              Actor[F, A](name, b(Behaviors()), actor.parentPath :+ actor.name, actor.supervisor) >>= { actorRef =>
                onBuild.complete(actorRef).as(actorRef.link())
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

    override def pipe(msg: T): F[Unit] = actor.push(msg)
    override def signal(signal: Signal): F[Unit] = actor.push(signal)
    override def stopChild(name: String): F[Unit] = actor.push(Command.StopChild(name))
    override def log: Logger[F] = ???
  }

  object IOContext {}

  final case class Actor[F[_], T](
      name: String,
      parentPath: Seq[String],
      queue: Queue[F, Command[F, T]],
      supervisor: Supervisor[F]
  )(implicit A: Async[F])
      extends ActorRef[F, T] {
    private lazy val _path = s"${parentPath.mkString("/")}/$name"

    def push(cmd: Command[F, T]): F[Unit] = queue.offer(cmd)
    def link(): Link[F] = ???
    def wide: ActorRef[F, T] = this

    override def path: String = _path

    override def ask[A](f: ActorRef[F, A] => T, timeout: FiniteDuration): F[A] = ???

    override def send(msg: T): F[Unit] = push(msg)
  }

  object Actor {

    def apply[F[_], T](
        name: String,
        behavior: Behavior[F, T],
        parentPath: Seq[String],
        supervisor: Supervisor[F]
    )(implicit A: Async[F]): F[Actor[F, T]] = {
      for {
        queue <- Queue.bounded[F, Command[F, T]](10)
        shutdownSignal <- SignallingRef[F].of(false)
        actor = Actor[F, T](name, parentPath, queue, supervisor)
        context = IOContext[F, T](actor)
        _ <- supervisor.supervise(
          Stream
            .fromQueueUnterminated(queue, 10)
            .evalScan[F, State[F, T]](
              State.Init[F, T](
                behavior,
                context,
                shutdownSignal.set(true),
                Map.empty
              )
            ) { case (state, command) =>
              state.reduce(command)
            }
            .interruptWhen(shutdownSignal)
            .compile
            .drain
        )

      } yield ???
    }

  }

}
