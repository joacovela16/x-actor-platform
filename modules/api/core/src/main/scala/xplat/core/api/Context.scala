package xplat.core.api

trait Context[F[_], T] {
  def name: String
  def path: String
  def self: ActorRef[F, T]
  def spawn[A](name: String)(b: Behaviors[F, A] => Behavior[F, A]): F[ActorRef[F, A]]
  def pipe(msg: T): F[Unit]
  def signal(signal: Signal): F[Unit]
  def stopChild(name: String): F[Unit]
  def log: Logger[F]
  def stop():F[Unit]
}
