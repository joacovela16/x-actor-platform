package xplat.core.api

trait Stash[F[_], T] {
  def isEmpty: F[Boolean]
  def size(): F[Int]
  def push(message: T): F[Unit]
  def unstashAll(): F[Unit]
  def unstashAll(b: => Behavior[F, T]): F[Behavior[F, T]]
}
