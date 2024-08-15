package xplat.core.api

trait Actor[F[_], T] {
  def name: String
  def push(msg: T): F[Unit]
  def push(signal: Signal): F[Unit]
}
