package xplat.core.api

import scala.concurrent.duration.FiniteDuration

trait Context[F[_], T] {
  def name: String
  def path: String
  def self: ActorRef[F, T]
  def spawn[A](name: String)(b: BehaviorBuilder[F, A]): F[ActorRef[F, A]]
  def pipe(msg: T): F[Unit]
  def signal(signal: Signal): F[Unit]
  def stopChild(name: String): F[Unit]
  def schedule[A](f: => F[A], delay: FiniteDuration): F[Cancelable[F]]
  def log: Logger[F]
  def stop():F[Unit]
}
