package xplat.core.api

import scala.concurrent.duration.FiniteDuration

trait ActorSystem[F[_]] {
  def spawn[T](name: String)(builder: Behaviors[F, T] => Behavior[F, T]): F[ActorRef[F, T]]
  def schedule[T](f: => F[T], delay: FiniteDuration): F[Cancelable[F]]
  def terminate(): F[Unit]
}

object ActorSystem {
  def apply[F[_]](implicit as: ActorSystem[F]): ActorSystem[F] = as
}
