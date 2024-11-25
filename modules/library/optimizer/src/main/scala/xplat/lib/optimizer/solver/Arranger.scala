package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}
import xplat.lib.optimizer.unapply.{GetVar, IsAddition, IsBinary, IsConstant}

object Arranger {

  def apply(expr: Expr.Numerical): Expr.Numerical = {
    val f = flatten(expr)
    val s = f.sortWith {
      case (GetVar(a), GetVar(b)) =>
        a < b
      case (_, IsConstant(_)) => true
      case (IsConstant(_), _) => false

    }
    val r = rebuild(s)
    r
  }

  private def rebuild(items: Seq[Expr.Numerical]): Expr.Numerical = {

    items match {
      case head +: tail =>
        tail.foldLeft(head) { case (acc, item) =>
          acc + item
        }
    }
  }

  private def flatten(expr: Expr.Numerical): Seq[Expr.Numerical] = {
    expr match {
      case IsConstant(e) =>
        Seq(e)
      case IsBinary(e1, Arithmentics.Times, IsBinary(e2, IsAddition(op), e3)) =>
        flatten(e1 * e2) ++ flatten({
          val sign = op.sign
          if (sign == 1) e1 else -1 * e1
        } * e3)
      case IsBinary(e1, IsAddition(op), e2) =>
        val sign = op.sign
        flatten(e1) ++ flatten(e2).map(e => if (sign == 1) e else sign * e)
      case e =>
        Seq(e)
    }
  }

}
