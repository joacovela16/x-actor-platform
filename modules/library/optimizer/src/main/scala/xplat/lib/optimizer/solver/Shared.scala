package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.Context.Data
import xplat.lib.optimizer.model.dsl.{BinaryOperator, BinaryRelation, Context, Expr}

object Shared {

  def evalForAll(context: Context, forall: Expr.Constraint.Forall): LazyList[Context] = {
    context.getStream(forall.domain.map(_.id)).filter(c => evalBoolean(c, forall.condition))
  }

  def evalDouble(context: Context, expr: Expr.Numerical): Double = {
    expr match {
      case Expr.Numerical.Binary(left, operator, right) =>
        val l = evalDouble(context, left)
        val r = evalDouble(context, right)
        operator match {
          case BinaryOperator.Plus =>
            l + r
          case BinaryOperator.Minus =>
            l - r
          case BinaryOperator.Times =>
            l * r
          case BinaryOperator.Divide =>
            l / r
        }
      case Expr.Numerical.Sum(domain, condition, body) =>
        context.withoutArgs.getStream(domain.map(_.id)).filter(c => evalBoolean(c, condition)).map(c => evalDouble(c, body)).sum
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
      case Expr.Numerical.GetVar(references, over) =>
        val indexed = context.args
        val suffix = references.map(i => indexed(i.referenceTo)).mkString
        val id = s"${over.id}_$suffix"
        context.variables(id)
      case Expr.Numerical.Constant(data) =>
        data
    }
  }

  def evalBoolean(context: Context, expr: Expr.Logical): Boolean = {
    expr match {
      case Expr.Logical.Relation(left, operator, right) =>
        val l = evalDouble(context, left)
        val r = evalDouble(context, right)

        operator match {
          case BinaryRelation.Equals =>
            l == r
          case BinaryRelation.Less =>
            l < r
          case BinaryRelation.LessEqual =>
            l <= r
          case BinaryRelation.Great =>
            l > r
          case BinaryRelation.GreatEqual =>
            l >= r
        }
      case Expr.Logical.Constant(data) =>
        data
    }
  }

}
