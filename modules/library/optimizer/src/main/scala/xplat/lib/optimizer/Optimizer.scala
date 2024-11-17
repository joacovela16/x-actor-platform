package xplat.lib.optimizer

import cats.Monad
import xplat.lib.optimizer.model.{Context, Model}

object Optimizer {

  def apply[F[_]](model: Model)(c: Context)(implicit M: Monad[F]): F[Unit] = {

    ???
  }



}
