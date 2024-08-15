package xplat.core.api

import scala.concurrent.duration.FiniteDuration

final case class Behaviors[F[_], T](dummy: Boolean = true) {
  def message(f: T => Behavior[F, T]): Behavior[F, T] = Behavior.OnMessage(f)
  def setup(f: Context[F, T] => Behavior[F, T]): Behavior[F, T] = Behavior.OnContext(f)

  def stash(size: Int)(f: Stash[F, T] => Behavior[F, T])(implicit as: ActorSystem[F]): Behavior[F, T] = {
    ???
  }

  def timer(time: FiniteDuration, message: T)(f: Cancelable[F] => Behavior[F, T])(implicit as: ActorSystem[F]): Behavior[F, T] = {
    ???
  }

  def pipe(f: => F[Behavior[F, T]]): Behavior[F, T] = Behavior.Wrapper(f)
  def same: Behavior[F, T] = Behavior.Same()
  def stop: Behavior[F, T] = Behavior.Stop()
}

object Behaviors {
  def apply[F[_], T]: Behaviors[F, T] = Behaviors()
}
