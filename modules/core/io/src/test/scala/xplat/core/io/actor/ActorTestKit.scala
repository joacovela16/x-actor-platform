package xplat.core.io.actor

import cats.effect.IO
import xplat.core.api.ActorSystem

import scala.concurrent.duration._

/** Small helpers shared by the IO actor tests. */
object ActorTestKit {

  val Timeout: FiniteDuration = 5.seconds

  /** Runs `f` against a fresh actor system and then shuts it down.
    *
    * The body is bounded by [[Timeout]] so a deadlocked actor fails the test instead of hanging the suite. On success the
    * shutdown is awaited (also bounded), which checks that the system terminates cleanly. On failure the shutdown is only
    * started in the background, because a stuck actor would make it hang forever.
    */
  def withSystem[A](f: ActorSystem[IO] => IO[A]): IO[A] =
    IOActorSystem[IO]("test").allocated.flatMap { case (system, release) =>
      f(system).timeout(Timeout).attempt.flatMap { result =>
        release.start.flatMap { shutdown =>
          result match {
            case Right(value) => shutdown.joinWithNever.timeout(Timeout).as(value)
            case Left(error)  => IO.raiseError(error)
          }
        }
      }
    }

  /** Re-evaluates `io` until its result satisfies `p`. Bound it with a timeout. */
  def awaitUntil[A](io: IO[A])(p: A => Boolean): IO[A] =
    io.flatMap(a => if (p(a)) IO.pure(a) else IO.sleep(10.millis) >> awaitUntil(io)(p))

  /** Completes once `send` starts failing, i.e. once the target actor is down. Returns the error. */
  def awaitStopped(send: IO[Unit]): IO[Throwable] =
    send.attempt.flatMap {
      case Left(error) => IO.pure(error)
      case Right(_)    => IO.sleep(10.millis) >> awaitStopped(send)
    }
}
