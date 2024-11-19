package xplat.lib.optimizer.solver.simplex

import cats.Monad
import cats.data.State
import xplat.lib.optimizer.model.core.{Solver, Stage}
import xplat.lib.optimizer.model.dsl.Expr.{Constraint, Logical, Numerical}
import xplat.lib.optimizer.model.dsl.{BinaryRelation, Expr}

final case class SimplexMain[F[_]]()(implicit M: Monad[F]) extends Solver[F] {

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

  override def solve(stage: Stage): F[Unit] = {
    val model = stage.model
    val context = stage.context

    final case class Variable(id: String, factor: Double)

    def evalNumerical(expr: Expr.Numerical): Double = expr match {
      case Numerical.Binary(left, operator, right) => ???
      case Numerical.Sum(domain, condition, body)  => ???
      case Numerical.GetParam(references, over)    => ???
      case Numerical.GetVar(references, over)      => ???
      case Numerical.Constant(data) =>
        data
    }

    def evalBool(expr: Expr.Logical): Boolean = {
      expr match {
        case Logical.Relation(left, operator, right) =>
          val l = evalNumerical(left)
          val r = evalNumerical(right)

          operator match {
            case BinaryRelation.Equals     => l == r
            case BinaryRelation.Less       => l < r
            case BinaryRelation.LessEqual  => l <= r
            case BinaryRelation.Great      => l > r
            case BinaryRelation.GreatEqual => l >= r
          }
        case Logical.Constant(data) =>
          data
      }
    }

    model.constraints.collect {
      case Constraint.Forall(domain, condition, body) =>
        context.getStream(domain.map(_.id))
        ???
      case Constraint.Relation(left, operator, right) => ???
    }

    ???
  }

}
