package xplat.core.cluster.api

import xplat.core.api.{Codec, Defaults, Ref}

import scala.concurrent.duration.FiniteDuration

/** Reference to an actor that may run on another node (a service found through the receptionist, a singleton or a
  * sharded entity).
  *
  * Unlike a local `ActorRef`, `ask` needs a codec for the reply, because the answer may come from another node.
  */
trait ClusterRef[F[_], T] {

  /** Full path of the actor, including the address of the node it runs on. */
  def path: String

  def send(msg: T): F[Unit]
  def !(msg: T): F[Unit] = send(msg)

  def ask[A](f: Ref[A] => T, timeout: FiniteDuration = Defaults.ASK_TIMEOUT)(implicit codec: Codec[A, Array[Byte]]): F[A]
  def ?[A](f: Ref[A] => T, timeout: FiniteDuration = Defaults.ASK_TIMEOUT)(implicit codec: Codec[A, Array[Byte]]): F[A] = ask(f, timeout)
}
