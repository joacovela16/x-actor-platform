package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}
import xplat.lib.optimizer.unapply.{IsAddition, IsBinary, IsConstant, IsTerm, IsVarFactor, WithAdditionFactor}

object Arranger {

  def apply(expr: Expr.Numerical): Expr.Numerical = {
    val f = flatten(expr)
    val s = f.sortWith {
      case (IsVarFactor(_, a), IsVarFactor(_, b)) =>
        a.id < b.id
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
      case IsTerm(e) =>
        Seq(e)
      case IsBinary(e1, IsAddition(op), e2) =>
        val sign = {
          op match {
            case Arithmentics.Plus  => 1
            case Arithmentics.Minus => -1
          }
        }
        flatten(e1) ++ flatten(e2).map(e => if (sign == 1) e else sign * e)
    }
  }

}
