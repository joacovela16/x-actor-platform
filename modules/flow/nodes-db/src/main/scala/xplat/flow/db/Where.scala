package xplat.flow.db

import io.circe.Json

/** The `WHERE` of `db.update` and `db.delete`: columns equal to values, all of them. */
object Where {

  /** The conditions, and the values that are parameters: a column whose value is `null` is `IS NULL` and has no parameter. */
  def build(columns: List[(String, Json)]): (List[String], List[Json]) =
    columns.foldLeft((List.empty[String], List.empty[Json])) { case ((conditions, params), (column, value)) =>
      if (value.isNull) (conditions :+ s"$column IS NULL", params) else (conditions :+ s"$column = ?", params :+ value)
    }

  def clause(conditions: List[String]): String = if (conditions.isEmpty) "" else conditions.mkString(" WHERE ", " AND ", "")
}
