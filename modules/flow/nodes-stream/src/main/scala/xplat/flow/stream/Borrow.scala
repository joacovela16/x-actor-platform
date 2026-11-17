package xplat.flow.stream

import cats.effect.{Async, Deferred, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._

/** Turns a lender, a `use(f)` that lends something only while `f` runs (the pools of the server lend clients that way), into a
  * [[Resource]], so that a source can keep what it borrowed for as long as its stream lasts.
  */
object Borrow {

  def apply[F[_], C](lend: (C => F[Unit]) => F[Unit])(implicit F: Async[F]): Resource[F, C] =
    Resource
      .make(
        for {
          got <- Deferred[F, Either[Throwable, C]]
          done <- Deferred[F, Unit]
          fiber <- F.start(lend(c => got.complete(Right(c)) >> done.get).handleErrorWith(e => got.complete(Left(e)).void))
          c <- F.onCancel(got.get.rethrow, done.complete(()).void >> fiber.cancel)
        } yield (c, done, fiber)
      ) { case (_, done, fiber) => done.complete(()) >> fiber.join.void }
      .map(_._1)
}
