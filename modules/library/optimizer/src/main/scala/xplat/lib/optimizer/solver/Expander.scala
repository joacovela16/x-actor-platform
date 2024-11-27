package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.{Context, ExpandedModel, Expr, Model}

object Expander {

  def apply(model: Model, context: Context): ExpandedModel = {
    ExpandedModel(
      kind = model.kind,
      objective = loop(model.objective, context),
      constraints = expandConstraints(model.constraints, context)
    )
  }

  private def expandConstraints(constraints: LazyList[Expr.Constraint], context: Context): LazyList[Expr.Predicate.Relation] = {
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
      case Expr.Constraint.Relation(body) =>
        val left = loop(body.left, context)
        val right = loop(body.right, context)
        Seq(Simplifier(Expr.Predicate.Relation(left, body.operator, right)))
    }
  }

  private def loop(expr: Expr.Numerical, context: Context): Expr.Numerical = {
    expr match {
      case Expr.Numerical.Binary(left, operator, right) =>
        Expr.Numerical.Binary(loop(left, context), operator, loop(right, context))
      case Expr.Numerical.Summatory(domain, condition, body) =>
        context
          .getStream(domain.map(_.id))
          .filter(c => Evaluators.evalBoolean(condition, c))
          .map(c => loop(body, c))
          .reduce(_ + _)
      case param: Expr.Numerical.GetParam =>
        Evaluators.evalDouble(param, context)
      case Expr.Numerical.GetVar(references, over) =>
        val indexed = context.args
        val suffix = references.map(i => indexed(i.referenceTo)).mkString
        val id = s"${over.id}_$suffix"
        Expr.Numerical.GetVar(references, over.withIndexedId(id))
      case e =>
        e
    }
  }

}
