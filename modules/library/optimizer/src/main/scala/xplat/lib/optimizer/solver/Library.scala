package xplat.lib.optimizer.solver

import cats.Monad
import xplat.lib.optimizer.model.core.Solver
import xplat.lib.optimizer.solver.simplex.SimplexMain

object Library {

  def apply[F[_]]()(implicit M: Monad[F]): Seq[Solver[F]] = Seq(
    SimplexMain[F]()
  )

}
