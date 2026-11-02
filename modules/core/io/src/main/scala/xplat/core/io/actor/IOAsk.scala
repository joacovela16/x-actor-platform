package xplat.core.io.actor

import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Dispatcher
import cats.implicits._
import xplat.core.api.ReplyTo

import scala.concurrent.duration.FiniteDuration

/** Request-response over a one-way `send`: the message carries a [[Ref]] the receiver answers through. */
private[actor] object IOAsk {

  def apply[F[_], T, A](send: T => F[Unit])(f: ReplyTo[A] => T, timeout: FiniteDuration)(implicit A: Async[F]): F[A] = {
    Dispatcher.sequential[F].use { d =>
      for {
        onMessage <- Deferred[F, A]
        ref = new ReplyTo[A] {
          def send(value: A): Unit = {
            d.unsafeRunAndForget(
              onMessage.complete(value)
            )
          }
        }
        _ <- send(f(ref))
        result <- A
          .race(A.sleep(timeout) >> A.pure(new Exception("Ask timeout")), onMessage.get)
          .flatMap {
            case Left(value) =>
              A.raiseError[A](value)
            case Right(value) =>
              A.pure(value)
          }
      } yield result
    }
  }

}
