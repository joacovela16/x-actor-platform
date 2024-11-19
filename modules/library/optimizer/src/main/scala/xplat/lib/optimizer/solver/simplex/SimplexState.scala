package xplat.lib.optimizer.solver.simplex

import cats.data.State

final case class SimplexState(tableau: Tableau)

object SimplexState {
  type Simplex[S] = State[SimplexState, S]

  def findPivotColumn(tableau: Tableau): Option[Int] =
    tableau.rows.last.init.zipWithIndex.minByOption(_._1).map(_._2)

  def findPivotRow(tableau: Tableau, pivotCol: Int): Option[Int] = {
    tableau.rows.init.zipWithIndex
      .collect { case (row, idx) if row(pivotCol) > 0 => (row.last / row(pivotCol), idx) }
      .minByOption(_._1)
      .map(_._2)
  }

  def pivot(pivotRow: Int, pivotCol: Int): Simplex[Unit] = State.modify { state =>
    val pivotValue = state.tableau.rows(pivotRow)(pivotCol)
    val updatedRow = Tableau.updateRow(state.tableau.rows(pivotRow), pivotCol, pivotValue)
    val updatedTableau = Tableau.updateOtherRows(Tableau(state.tableau.rows.updated(pivotRow, updatedRow)), pivotRow, pivotCol)
    state.copy(tableau = updatedTableau)
  }

  def isOptimal(tableau: Tableau): Boolean = tableau.rows.last.init.forall(_ >= 0)

  def extractSolution(tableau: Tableau): List[Double] = {
    val numVariables = tableau.rows.head.length - 1
    val solution = Array.fill(numVariables - 1)(0.0)
    tableau.rows.foreach { row =>
      val basicVariableIdx = row.init.indexWhere(_ == 1.0)
      if (basicVariableIdx >= 0 && row.count(_ != 0.0) == 2) {
        solution(basicVariableIdx) = row.last
      }
    }
    solution.toList
  }

}
