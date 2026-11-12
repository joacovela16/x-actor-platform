package xplat.core.api

import cats.effect.Resource

import scala.concurrent.duration.FiniteDuration

trait ActorSystem[F[_]] {
  def name: String
  def spawn[T](name: String)(behavior: Behavior[F, T]): F[ActorRef[F, T]] = spawn(name, Mailbox.Unbounded)(behavior)
  def spawn[T](name: String, mailbox: Mailbox)(behavior: Behavior[F, T]): F[ActorRef[F, T]]
  def schedule[T](f: => F[T], delay: FiniteDuration): F[Cancelable[F]]
  def terminate(): F[Unit]
}

object ActorSystem {
  def apply[F[_]](implicit as: ActorSystemBuilder[F]): Resource[F, ActorSystem[F]] = as.build()
}
