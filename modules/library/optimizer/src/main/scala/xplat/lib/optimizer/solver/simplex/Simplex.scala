package xplat.lib.optimizer.solver.simplex

import cats.Monad
import cats.data.State
import xplat.lib.optimizer.model.core.{Solver, Stage}

final case class Simplex[F[_]]()(implicit M:Monad[F]) extends Solver[F]{


  def simplexLoop: SimplexState.Simplex[Option[List[Double]]] = State { state =>
    if (SimplexState.isOptimal(state.tableau)) {
      (state, Some(SimplexState.extractSolution(state.tableau)))
    } else {
      SimplexState.findPivotColumn(state.tableau) match {
        case Some(pivotCol) =>
          SimplexState.findPivotRow(state.tableau, pivotCol) match {
            case Some(pivotRow) =>
              val newState = SimplexState.pivot(pivotRow, pivotCol).runS(state).value
              simplexLoop.run(newState).value
            case None => (state, None) // Unbounded
          }
        case None => (state, None) // Optimal or infeasible
      }
    }
  }


  override def presolve(stage: Stage): F[Stage] = M.pure(stage)

  override def solve(stage: Stage): F[Stage] = {
    ???
  }
}
