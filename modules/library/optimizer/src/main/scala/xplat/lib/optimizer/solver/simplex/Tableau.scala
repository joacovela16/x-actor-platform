package xplat.lib.optimizer.solver.simplex

final case class Tableau(rows: List[List[Double]])

object Tableau {
  def updateRow(row: List[Double], pivotCol: Int, pivotValue: Double): List[Double] =
    row.map(_ / pivotValue)

  def updateOtherRows(tableau: Tableau, pivotRow: Int, pivotCol: Int): Tableau = {
    val pivotRowData = tableau.rows(pivotRow)
    val updatedRows = tableau.rows.zipWithIndex.map { case (row, i) =>
      if (i == pivotRow) row
      else {
        val factor = row(pivotCol)
        row.zip(pivotRowData).map { case (x, y) => x - factor * y }
      }
    }
    Tableau(updatedRows)
  }
}