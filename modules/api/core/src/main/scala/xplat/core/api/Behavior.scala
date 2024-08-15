package xplat.core.api

import scala.language.implicitConversions

sealed trait Behavior[F[_], T] {
  def onSignal: Behavior.OnSignalPartial[F, T] = Behavior.OnSignalPartial(this)
}

object Behavior {
  final case class OnMessage[F[_], T](f: T => Behavior[F, T]) extends Behavior[F, T]
  final case class OnContext[F[_], T](f: Context[F, T] => Behavior[F, T]) extends Behavior[F, T]
  final case class OnSignal[F[_], T](f: PartialFunction[Signal, Behavior[F, T]], nested: Behavior[F, T]) extends Behavior[F, T]
  final case class Same[F[_], T]() extends Behavior[F, T]
  final case class Stop[F[_], T]() extends Behavior[F, T]
  final case class Wrapper[F[_], T](effect: F[Behavior[F, T]]) extends Behavior[F, T]

  implicit def wrap[F[_], T](b: F[Behavior[F, T]]): Behavior[F, T] = Behavior.Wrapper(b)

  final case class OnSignalPartial[F[_], T](b: Behavior[F, T]) {
    def apply(f: PartialFunction[Signal, Behavior[F, T]]): Behavior[F, T] = Behavior.OnSignal(f, b)
  }

}
