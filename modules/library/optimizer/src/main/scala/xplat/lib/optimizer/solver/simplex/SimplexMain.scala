package xplat.lib.optimizer.solver.simplex

import cats.Monad
import cats.data.State
import com.softwaremill.quicklens.ModifyPimp
import xplat.lib.optimizer.model.core.{Solver, Stage}
import xplat.lib.optimizer.model.dsl.Expr.{Constraint, Logical, Numerical}
import xplat.lib.optimizer.model.dsl.{Relations, Context, Expr, Model}
import xplat.lib.optimizer.solver.Shared

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
      case Numerical.Summatory(domain, condition, body)  => ???
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
            case Relations.Equals     => l == r
            case Relations.Less       => l < r
            case Relations.LessEqual  => l <= r
            case Relations.Great      => l > r
            case Relations.GreatEqual => l >= r
          }
        case Logical.Constant(data) =>
          data
      }
    }

    model.constraints.collect {
      case Constraint.Forall(domain, condition, body) =>
        context.getStream(domain.map(_.id))
        ???
      case Constraint.Relation(body) => ???
    }

    ???
  }

}

object SimplexMain {
  final case class Variable(id: String, factor: Double, clearance: Boolean)

  final case class Entry(variables: List[Variable], independentTerm: Double) {
    def mapTerm(f: Double => Double): Entry = this.modify(_.independentTerm).using(f)
  }

  final case class Registry(
      clearance: Int,
      entries: List[Entry]
  ) {
    def withEntry(v: Entry): Registry = this.modify(_.entries).using(v +: _)
  }

  private def prepare(model: Model, context: Context): Registry = {

    def isStatic(expr: Expr.Numerical): Boolean = {
      expr match {
        case Expr.Numerical.Binary(left, operator, right) =>
          isStatic(left) && isStatic(right)
        case Expr.Numerical.Summatory(domain, condition, body) =>
          isStatic(body)
        case Expr.Numerical.GetParam(references, over) =>
          true
        case Expr.Numerical.GetVar(references, over) =>
          false
        case Expr.Numerical.Constant(_) => true
      }
    }

    def analyzeNumerical(
        context: Context,
        expr: Expr.Numerical,
        entry: Entry,
        sideSign: Double
    ): Entry = {
      if (expr.isStatic) {
        Shared.evalDouble(context, expr)
      }

      expr match {
        case Expr.Numerical.Binary(left, operator, right) =>
          val e = {
            if (left.isStatic && right.isStatic) {
              val r = Shared.evalDouble(context, expr)
              entry.mapTerm(x => x + r * sideSign)
            } else {}
            ???
          }

          val e1 = {
            if (left.isStatic) {
              val r1 = Shared.evalDouble(context, left)
              entry.mapTerm(x => x + sideSign * r1)
            } else {
              entry
            }
          }

          val e2 = {
            ???
          }
          ???
        case Expr.Numerical.Summatory(domain, condition, body) => ???
        case Expr.Numerical.GetParam(references, over)   => ???
        case Expr.Numerical.GetVar(references, over)     => ???
        case _                                           => ???
      }

      entry
    }

    val base = Registry(clearance = 0, entries = Nil)

    model.constraints.foldLeft(base) { case (acc, item) =>
      item match {
        case datum: Expr.Constraint.Forall =>
          Shared.evalForAll(context, datum).foldLeft(acc) { case (acc2, c) =>
            datum.body match {
              case datum: Expr.Logical.Relation =>
                val e1 = analyzeNumerical(context = c, expr = datum.left, entry = Entry(Nil, 0), sideSign = -1)
                val e2 = analyzeNumerical(context = c, expr = datum.right, entry = e1, sideSign = 1)
                acc2.withEntry(e2)
              case _ =>
                acc2
            }
          }
        case datum: Expr.Constraint.Relation =>
//          val e1 = analyzeNumerical(context = context, expr = datum.left, entry = Entry(Nil, 0), sideSign = -1)
//          val e2 = analyzeNumerical(context = context, expr = datum.right, entry = e1, sideSign = 1)
//          acc.withEntry(e2)
          ???
        case _ =>
          acc
      }
    }
  }

}
