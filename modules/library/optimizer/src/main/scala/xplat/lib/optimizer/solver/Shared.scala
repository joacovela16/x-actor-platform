package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.Context.Data
import xplat.lib.optimizer.model.dsl._

object Shared {

  def extractMetadata(expr: Expr.Constraint): Seq[Metadata] = {
    expr match {
      case Expr.Constraint.Forall(domain, condition, body) =>
        extractMetadata(body)
      case Expr.Constraint.Relation(body) =>
        extractMetadata(body)
    }
  }

  def extractMetadata(expr: Expr.Logical): Seq[Metadata] = {
    expr match {
      case Expr.Logical.Relation(left, _, right) =>
        val l = extractMetadata(left)
        val r = extractMetadata(right)

        l ++ r ++ {
          val c1 = l.contains(Metadata.DivisionByVar) && r.contains(Metadata.VarRef)
          val c2 = r.contains(Metadata.DivisionByVar) && l.contains(Metadata.VarRef)
          if (c1 || c2) Seq(Metadata.NoLinear) else Nil
        }
      case _ =>
        Nil
    }
  }

  def extractMetadata(expr: Expr.Numerical): Seq[Metadata] = {
    expr match {
      case Expr.Numerical.Sum(_, _, body) =>
        extractMetadata(body)
      case Expr.Numerical.Binary(left, operator, right) =>
        val l = extractMetadata(left)
        val r = extractMetadata(right)
        val base = l ++ r
        operator match {
          case BinaryOperator.Times =>
            val c1: Seq[Metadata] = if (r.contains(Metadata.VarRef) && l.contains(Metadata.VarRef)) Seq(Metadata.VarMultiplication, Metadata.NoLinear) else Nil
            base ++ c1

          case BinaryOperator.Divide =>
            val c1 = if (r.contains(Metadata.VarRef)) Seq(Metadata.DivisionByVar) else Nil
            base ++ c1
          case _ =>
            val c1 = l.contains(Metadata.DivisionByVar) && r.contains(Metadata.VarRef)
            val c2 = r.contains(Metadata.DivisionByVar) && l.contains(Metadata.VarRef)
            val e1 = if (c1 || c2) Seq(Metadata.NoLinear) else Nil
            base ++ e1
        }
      case Expr.Numerical.GetVar(_, _) =>
        Seq(Metadata.VarRef)
      case _ =>
        Nil
    }
  }

  def hasVariable(context: Context, expr: Expr.Numerical): Boolean = {
    expr match {
      case Expr.Numerical.Binary(left, _, right) =>
        hasVariable(context, left) || hasVariable(context, right)
      case Expr.Numerical.Sum(_, _, body) =>
        hasVariable(context, body)
      case _: Expr.Numerical.GetVar =>
        true
      case _ =>
        false
    }
  }

  def isLineal(context: Context, expr: Expr.Numerical, onVar: Boolean): Boolean = {
    expr match {
      case Expr.Numerical.Binary(left, operator, right) =>
        operator match {
          case BinaryOperator.Plus =>
            isLineal(context, left, onVar) && isLineal(context, right, onVar)
          case BinaryOperator.Minus =>
            isLineal(context, left, onVar) && isLineal(context, right, onVar)
          case BinaryOperator.Times  => ???
          case BinaryOperator.Divide => ???
        }
      case Expr.Numerical.Sum(domain, condition, body) => ???
      case Expr.Numerical.GetVar(_, _) =>
        onVar
      case _ =>
        true
    }
  }

  def isLineal(context: Context, expr: Expr.Logical): Boolean = {
    expr match {
      case Expr.Logical.Relation(left, operator, right) => ???
      case _                                            => true
    }
  }

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
