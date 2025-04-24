package xplat.core.api

import scala.concurrent.duration.FiniteDuration

trait ActorRef[F[_], T] {
  def name: String
  def path: String
  def ask[A](f: Ref[A] => T, timeout: FiniteDuration = Defaults.ASK_TIMEOUT): F[A]
  def send(msg: T): F[Unit]
  def !(msg: T): F[Unit] = send(msg)
  def ?[A](f: Ref[A] => T, timeout: FiniteDuration = Defaults.ASK_TIMEOUT): F[A] = ask(f, timeout)
}
