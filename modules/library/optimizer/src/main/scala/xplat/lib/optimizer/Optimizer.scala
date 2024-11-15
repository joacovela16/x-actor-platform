package xplat.lib.optimizer

import cats.Monad
import xplat.lib.optimizer.model.Model

object Optimizer {
  def apply[F[_]](model: Model)(implicit M: Monad[F]): F[Unit] = {

    ???
  }
}
