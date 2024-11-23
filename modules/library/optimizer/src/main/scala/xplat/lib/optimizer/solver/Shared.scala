package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.Arithmentics.Times
import xplat.lib.optimizer.model.dsl.Context.Data
import xplat.lib.optimizer.model.dsl.Expr.{Logical, Numerical}
import xplat.lib.optimizer.model.dsl._

import scala.annotation.tailrec

object Shared {

  case object IsLiteral {

    def unapply(expr: Expr.Numerical): Option[Double] = {
      expr match {
        case Numerical.Constant(data) => Option(data)
        case _                        => None
      }
    }

  }

  case object IsAddition {

    def unapply(op: Arithmentics): Option[Arithmentics.Addition] = {
      op match {
        case Arithmentics.Plus  => Option(Arithmentics.Plus)
        case Arithmentics.Minus => Option(Arithmentics.Minus)
        case _ =>
          None
      }

    }

  }

  case object IsFactor {

    def unapply(op: Arithmentics): Option[Arithmentics.Factor] = {
      op match {
        case Arithmentics.Times  => Option(Arithmentics.Times)
        case Arithmentics.Divide => Option(Arithmentics.Divide)
        case _ =>
          None
      }
    }

  }

  case object Addition {

    def unapply(op: Arithmentics): Option[Arithmentics] = {
      op match {
        case Arithmentics.Plus  => Option(op)
        case Arithmentics.Minus => Option(op)
        case _ =>
          None
      }
    }

  }

  case object AdditionExpr {

    def unapply(expr: Expr.Numerical): Option[(Expr.Numerical, Arithmentics.Addition, Expr.Numerical)] = {
      expr match {
        case Numerical.Binary(left, IsAddition(op), right) =>
          Option((left, op, right))
        case _ =>
          None
      }
    }

  }

  case object IsConstant {

    def unapply(expr: Expr.Numerical): Option[Expr.Numerical] = {
      expr match {
        case Numerical.GetParam(_, _) =>
          Option(expr)
        case Numerical.Constant(_) =>
          Option(expr)
        case Numerical.Binary(IsConstant(_), _, IsConstant(_)) =>
          Option(expr)
        case _ =>
          None
      }
    }

  }

  case object IsVar {

    def unapply(expr: Expr.Numerical): Option[Expr.Numerical.GetVar] = {
      expr match {
        case e: Expr.Numerical.GetVar => Option(e)
        case _                        => None
      }
    }

  }

  case object IsVarFactor {

    def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical.GetVar)] = {
      expr match {
        case e: Expr.Numerical.GetVar =>
          Option((1, e))
        case Expr.Numerical.Binary(IsLiteral(a), Arithmentics.Times, IsVar(b)) =>
          Option(a -> b)
        case Expr.Numerical.Binary(IsVar(b), IsFactor(_), IsLiteral(a)) =>
          Option(a -> b)
        case Expr.Numerical.Binary(IsVar(b), Arithmentics.Divide, IsLiteral(a)) =>
          Option((1 / a) -> b)
        case Expr.Numerical.Binary(IsLiteral(a), Arithmentics.Times, IsVarFactor(b, c)) =>
          // 2 * (x/5) => 2/5 -> x
          Option(a * b -> c)
        case _ =>
          None
      }
    }

  }

  case object IsParam {

    def unapply(expr: Expr.Numerical): Option[Expr.Numerical.GetParam] = {
      expr match {
        case e: Expr.Numerical.GetParam => Option(e)
        case _                          => None
      }
    }

  }

  case object WithConstantFactor {

    def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical)] = {
      expr match {
        case IsBinary(IsLiteral(c), Arithmentics.Times, e) =>
          Option(c -> e)
        case IsBinary(e, Arithmentics.Times, IsLiteral(c)) =>
          Option(c -> e)
        case IsVar(e) =>
          Option(1.0 -> Expr.Numerical(e))
        case IsParam(e) =>
          Option(1.0 -> Expr.Numerical(e))

        case _ =>
          None
      }
    }

  }

  case object IsAdditionFactor {

    def unapply(expr: Expr.Numerical): Option[(Double, Expr.Numerical, Double, Expr.Numerical)] = {
      expr match {
        case IsBinary(WithConstantFactor(f1, e1), IsAddition(op), WithConstantFactor(f2, e2)) =>
          val sign = {
            op match {
              case Arithmentics.Plus  => 1
              case Arithmentics.Minus => -1
            }
          }
          Option((f1, e1, sign * f2, e2))
        case _ => None
      }
    }

  }

  case object IsBinary {

    def unapply(expr: Expr.Numerical): Option[(Expr.Numerical, Arithmentics, Expr.Numerical)] = {
      expr match {
        case Numerical.Binary(left, operator, right) =>
          Option((left, operator, right))
        case _ =>
          None
      }
    }

  }

  @tailrec
  def simplify(expr: Expr.Logical): Expr.Logical = {
    expr match {
      // a + 5 <= 7 => a <= 7 - 5
      case Logical.Relation(IsBinary(a, IsAddition(op), IsConstant(c)), operator, right) =>
        simplify(
          Logical.Relation(
            simplify(a),
            operator, {

              val sign = op match {
                case Arithmentics.Plus =>
                  -1
                case Arithmentics.Minus =>
                  1
              }

              simplify(
                Expr.Numerical.Binary(
                  Expr.Numerical.Binary(sign, Arithmentics.Times, c),
                  Arithmentics.Plus,
                  right
                )
              )
            }
          )
        )
      case e =>
        e
    }
  }

  def simplify(expr: Expr.Constraint): Expr.Constraint = {
    expr match {
      case Expr.Constraint.Forall(domain, condition, body) =>
        Expr.Constraint.Forall(domain, condition, simplify(body))
      case Expr.Constraint.Relation(body) =>
        Expr.Constraint.Relation(simplify(body))
    }
  }

  def simplify(expr: Expr.Numerical): Expr.Numerical = {
    expr match {
      // (x + y) + y => x + 2y
      case IsBinary(IsBinary(l1, Arithmentics.Plus, IsVarFactor(f1, v1)), Arithmentics.Plus, IsVarFactor(f2, v2)) if v1.id == v2.id =>
        simplify(
          Expr.Numerical.Binary(
            l1,
            Arithmentics.Plus,
            (f1 + f2) * v1
          )
        )
      // 2 * (a + b) => 2*a + 2*b
      case IsBinary(IsConstant(data), Arithmentics.Times, AdditionExpr(l, op, r)) =>
        simplify(
          Expr.Numerical.Binary(
            left = Expr.Numerical.Binary(data, Times, l),
            operator = op,
            right = Expr.Numerical.Binary(data, Times, r)
          )
        )
      // (a + b)*2 => 2*a + 2*b
      case IsBinary(AdditionExpr(l, op, r), Arithmentics.Times, IsConstant(data)) =>
        simplify(
          Expr.Numerical.Binary(
            left = Expr.Numerical.Binary(data, Times, l),
            operator = op,
            right = Expr.Numerical.Binary(data, Times, r)
          )
        )
      case IsBinary(IsLiteral(a), op, IsLiteral(b)) =>
        op match {
          case Arithmentics.Plus   => a + b
          case Arithmentics.Minus  => a - b
          case Arithmentics.Times  => a * b
          case Arithmentics.Divide => a / b
        }
      // 2a - a => a
      case IsAdditionFactor(f1, IsVar(a), f2, IsVar(b)) if a.id == b.id =>
        simplify((f1 + f2) * a)
      // b +/- a => -/+ a + b
      case IsAdditionFactor(f1, IsVar(b), f2, IsVar(a)) if a.id < b.id =>
        simplify {
          (f2 * a) + (f1 * b)
        }
      // x3 + x2 + x1 => x1 + x2 + x3
      // 2*x3 + x2 + x1 => x1 + x2 + 2*x3
      case IsAdditionFactor(f1, IsAdditionFactor(f2, IsVar(v1), f3, IsVar(v2)), f4, IsVar(v3)) if v3.id < v1.id && v3.id < v2.id =>
        simplify(
          (f4 * v3) + f1 * ((f2 * v1) + (f3 * v2))
        )
      case IsBinary(IsBinary(IsVarFactor(f1, v1), IsAddition(op1), IsVarFactor(f2, v2)), IsAddition(op2), IsVarFactor(f3, v3)) if v3.id < v1.id && v3.id < v2.id =>
        simplify(
          Expr.Numerical.Binary(
            simplify(f3 * v3),
            op2,
            Expr.Numerical.Binary(
              simplify(f2 * v2),
              op1,
              simplify(f1 * v1)
            )
          )
        )

      // moves variables to left
      case IsAdditionFactor(f1, IsConstant(a), f2, IsVar(b)) =>
        simplify(
          (f2 * b) + (f1 * a)
        )
      case Expr.Numerical.Summatory(domain, condition, body) =>
        Expr.Numerical.Summatory(domain, condition, simplify(body))
      case IsBinary(IsLiteral(0), Arithmentics.Times, _) =>
        0
      case IsBinary(_, Arithmentics.Times, IsLiteral(0)) =>
        0
      case IsBinary(IsLiteral(1), Arithmentics.Times, e) =>
        simplify(e)
      case IsBinary(e, Arithmentics.Times, IsLiteral(1)) =>
        simplify(e)
      case IsBinary(a, op, b) =>
        Expr.Numerical.Binary(
          simplify(a),
          op,
          simplify(b)
        )
      case e =>
        e
    }
  }

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
          case Relations.Equals =>
            l == r
          case Relations.Less =>
            l < r
          case Relations.LessEqual =>
            l <= r
          case Relations.Great =>
            l > r
          case Relations.GreatEqual =>
            l >= r
        }
      case Expr.Logical.Constant(data) =>
        data
    }
  }

}
