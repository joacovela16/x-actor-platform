package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.Context.Data
import xplat.lib.optimizer.model.dsl.Expr.Predicate
import xplat.lib.optimizer.model.dsl.Expr.Predicate.Logical
import xplat.lib.optimizer.model.dsl._

object Evaluators {

  def evalBoolean(expr: Predicate, context: Context): Boolean = {
    expr match {
      case Predicate.Relation(left, operator, right) =>
        val l = evalDouble(left, context)
        val r = evalDouble(right, context)
        operator match {
          case Relations.Equals     => l == r
          case Relations.Less       => l < r
          case Relations.LessEqual  => l <= r
          case Relations.Great      => l > r
          case Relations.GreatEqual => l >= r
        }
      case logical: Predicate.Logical =>
        logical match {
          case Logical.Binary(left, operator, right) =>
            operator match {
              case BinaryLogicOps.And =>
                val l = evalBoolean(left, context)
                val r = evalBoolean(right, context)
                l && r
              case BinaryLogicOps.Or =>
                evalBoolean(left, context) || evalBoolean(right, context)
            }
          case Logical.Not(body) =>
            val b = evalBoolean(body, context)
            !b
          case Logical.Constant(data) =>
            data
        }
    }
  }

  def evalDouble(expr: Expr.Numerical, context: Context): Double = {
    expr match {
      case Expr.Numerical.Binary(left, operator, right) =>
        val l = evalDouble(left, context)
        val r = evalDouble(right, context)
        operator match {
          case Arithmentics.Plus =>
            l + r
          case Arithmentics.Minus =>
            l - r
          case Arithmentics.Times =>
            l * r
          case Arithmentics.Divide =>
            l / r
        }
      case Expr.Numerical.Summatory(domain, condition, body) =>
        context.withoutArgs.getStream(domain.map(_.id)).filter(c => Evaluators.evalBoolean(condition, c)).map(c => evalDouble(body, c)).sum
      case Expr.Numerical.GetParam(references, over) =>
        val indexed = context.args
        val suffix = references.map(i => indexed(i.referenceTo)).mkString

        context.input(over.id) match {
          case Data.Param1(data) =>
            data
          case Data.Param2(data) =>
            data(suffix)
          case _ =>
            ???
        }
      case Expr.Numerical.GetVar(_, over) =>
        context.variables(over.indexedId)
      case Expr.Numerical.Constant(data) =>
        data
    }
  }

}
