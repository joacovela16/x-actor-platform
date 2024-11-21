package xplat.lib.optimizer

import cats.Monad
import xplat.lib.optimizer.model.dsl.Expr.{Constraint, Logical, Numerical}
import xplat.lib.optimizer.model.dsl._
import xplat.lib.optimizer.solver.Shared

object Optimizer {

  def apply[F[_]](model: Model)(c: Context)(implicit M: Monad[F]): F[Unit] = {
    println(
      solver(model, c)
    )
    M.unit
  }

  private def solver[F[_]: Monad](model: Model, context: Context): Long = {
    println(context)
    val c = countConstraint(model, context)
    println(
      model.constraints.flatMap(c => Shared.extractMetadata(c))
    )
    c
  }

  private def countConstraint(model: Model, context: Context): Long = {
    model.constraints.map {
      case Constraint.Forall(domain, condition, _) =>
        context.getStream(domain.map(_.id)).count(c => evaluateBoolean(c, condition))
      case _ =>
        1
    }.sum
  }

  private def loadVariables(expr: Expr.Numerical, base: Map[String, Int], context: Context): Map[String, Int] = {
    expr match {
      case Expr.Numerical.Binary(left, operator, right) => ???
      case Expr.Numerical.Sum(domain, condition, body) =>
        ???
      case Numerical.GetParam(references, over) => ???
      case Numerical.GetVar(references, over)   => ???
      case Numerical.Constant(data)             => ???
    }
  }

  private def countVariables(model: Model, context: Context): Long = {

    def countVar(expr: Expr.Numerical): Long = {
      expr match {
        case Numerical.Binary(left, operator, right) =>
          ???
        case Numerical.Sum(domain, condition, body) =>
          ???
        case Numerical.GetParam(references, over) =>
          ???
        case Numerical.GetVar(references, over) =>
          ???
        case _ =>
          ???
      }
    }

    /*model.constraints.map { c =>
      c.body match {
        case Logical.Forall(domain, condition, body) =>
          ???
        case Logical.Relation(left, operator, right) =>
          ???
        case _ =>
          0
      }
    }*/

    ???
  }

  private def evaluateBoolean(context: Context, expr: Expr.Logical): Boolean = {
    expr match {
      case Logical.Relation(left, operator, right) =>
        val l = evaluateDouble(context, left)
        val r = evaluateDouble(context, right)
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

  private def evaluateDouble(context: Context, expr: Expr.Numerical): Double = {
    expr match {
      case Numerical.Binary(left, operator, right) =>
        val l = evaluateDouble(context, left)
        val r = evaluateDouble(context, right)
        operator match {
          case BinaryOperator.Plus   => l + r
          case BinaryOperator.Minus  => l - r
          case BinaryOperator.Times  => l * r
          case BinaryOperator.Divide => l / r
        }
      case Numerical.Sum(domain, condition, body) =>
        context.getStream(domain.map(_.id)).filter(c => evaluateBoolean(c, condition)).map(c => evaluateDouble(c, body)).sum
      case Numerical.GetParam(references, over) => ???
      case Numerical.GetVar(references, over) =>
        ???
      case Numerical.Constant(data) =>
        data
    }
  }

}
