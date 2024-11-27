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

  def extractMetadata(expr: Expr.Predicate): Seq[Metadata] = {
    expr match {
      case Expr.Predicate.Relation(left, _, right) =>
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
      case Expr.Numerical.Summatory(_, _, body) =>
        extractMetadata(body)
      case Expr.Numerical.Binary(left, operator, right) =>
        val l = extractMetadata(left)
        val r = extractMetadata(right)
        val base = l ++ r
        operator match {
          case Arithmentics.Times =>
            val c1: Seq[Metadata] = if (r.contains(Metadata.VarRef) && l.contains(Metadata.VarRef)) Seq(Metadata.VarMultiplication, Metadata.NoLinear) else Nil
            base ++ c1

          case Arithmentics.Divide =>
            val c1 = if (r.contains(Metadata.VarRef)) Seq(Metadata.DivisionByVar) else Nil
            base ++ c1
          case _ =>
            val c1 = l.contains(Metadata.DivisionByVar) && r.contains(Metadata.VarRef)
            val c2 = r.contains(Metadata.DivisionByVar) && l.contains(Metadata.VarRef)
            val e1 = if (c1 || c2) Seq(Metadata.NoLinear) else Nil
            base ++ e1
        }
      case Expr.Numerical.GetVar(_, over) =>
        Seq(Metadata.VarRef)
      case _ =>
        Nil
    }
  }

  def hasVariable(context: Context, expr: Expr.Numerical): Boolean = {
    expr match {
      case Expr.Numerical.Binary(left, _, right) =>
        hasVariable(context, left) || hasVariable(context, right)
      case Expr.Numerical.Summatory(_, _, body) =>
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
          case Arithmentics.Plus =>
            isLineal(context, left, onVar) && isLineal(context, right, onVar)
          case Arithmentics.Minus =>
            isLineal(context, left, onVar) && isLineal(context, right, onVar)
          case Arithmentics.Times  => ???
          case Arithmentics.Divide => ???
        }
      case Expr.Numerical.Summatory(domain, condition, body) => ???
      case Expr.Numerical.GetVar(_, _) =>
        onVar
      case _ =>
        true
    }
  }

  def evalForAll(context: Context, forall: Expr.Constraint.Forall): LazyList[Context] = {
    context.getStream(forall.domain.map(_.id)).filter(c => Evaluators.evalBoolean(forall.condition, c))
  }



}
