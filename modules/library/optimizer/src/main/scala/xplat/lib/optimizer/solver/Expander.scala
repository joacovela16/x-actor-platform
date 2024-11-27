package xplat.lib.optimizer.solver

import com.softwaremill.quicklens.ModifyPimp
import xplat.lib.optimizer.model.dsl.Expr.{Constraint, Numerical}
import xplat.lib.optimizer.model.dsl.{Context, Expr, Model}

object Expander {

  def apply(model: Model, context: Context): Model = {
    model
      .modify(_.constraints)
      .using(xs => expandConstraints(xs, context))
  }

  private def expandConstraints(constraints: LazyList[Expr.Constraint], context: Context): LazyList[Expr.Constraint] = {
    constraints.flatMap {
      case Expr.Constraint.Forall(domain, condition, body) =>
        context
          .getStream(domain.map(_.id))
          .filter(c => Evaluators.evalBoolean(condition, c))
          .map { c =>
            val left = loop(body.left, c)
            val right = loop(body.right, c)
            Simplifier(Expr.Predicate.Relation(left, body.operator, right))
          }
      case Constraint.Relation(body) =>
        val left = loop(body.left, context)
        val right = loop(body.right, context)
        Seq(Simplifier(Expr.Predicate.Relation(left, body.operator, right)))
    }
  }

  private def loop(expr: Expr.Numerical, context: Context): Expr.Numerical = {
    expr match {
      case Numerical.Binary(left, operator, right) =>
        Numerical.Binary(loop(left, context), operator, loop(right, context))
      case Numerical.Summatory(domain, condition, body) =>
        context
          .getStream(domain.map(_.id))
          .filter(c => Evaluators.evalBoolean(condition, c))
          .map(c => loop(body, c))
          .reduce(_ + _)
      case param: Numerical.GetParam =>
        Evaluators.evalDouble(param, context)
      case e =>
        e
    }
  }

}
