package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.Expr.Logical
import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr}
import xplat.lib.optimizer.unapply._

import scala.annotation.tailrec

object Simplifier {

  @tailrec
  def simplify(expr: Expr.Logical): Expr.Logical = {
    expr match {
      // a + 5 <= 7 => a <= 7 - 5
      case Logical.Relation(IsBinary(a, IsAddition(op), IsConstant(c)), operator, right) =>
        simplify(
          Logical.Relation(
            Simplifier(a),
            operator, {

              val sign = op match {
                case Arithmentics.Plus =>
                  -1
                case Arithmentics.Minus =>
                  1
              }

              Simplifier(
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

  def apply(expr: Expr.Numerical): Expr.Numerical = {
    simplifyLoop(
      Arranger(expr)
    )
  }

  private def simplifyLoop(expr: Expr.Numerical): Expr.Numerical = {
    expr match {
      // ************** BASE **************//
      case Expr.Numerical.Summatory(domain, condition, body) =>
        Expr.Numerical.Summatory(domain, condition, simplifyLoop(body))
      case IsBinary(IsLiteral(0), Arithmentics.Times, _) =>
        0
      case IsBinary(_, Arithmentics.Times, IsLiteral(0)) =>
        0
      case IsBinary(e, Arithmentics.Times, IsLiteral(1)) =>
        simplifyLoop(e)
      case IsBinary(IsLiteral(1), Arithmentics.Times, e) =>
        simplifyLoop(e)
      // (? + y) + y => x + 2y
      case IsBinary(IsBinary(l1, IsAddition(op1), IsVarFactor(f1, v1)), IsAddition(op2), IsVarFactor(f2, v2)) if v1.id == v2.id =>
        simplifyLoop(
          Expr.Numerical.Binary(
            simplifyLoop(l1),
            Arithmentics.Plus,
            op2.withSign(simplifyLoop((f1 + f2) * op1.withSign(v1)))
          )
        )

      // 2 * (a + b) => 2*a + 2*b
      case IsBinary(IsConstant(data), Arithmentics.Times, AdditionExpr(l, op, r)) =>
        simplifyLoop(
          Expr.Numerical.Binary(
            left = simplifyLoop(data * l),
            operator = op,
            right = simplifyLoop(data * r)
          )
        )
      // (a + b)*2 => 2*a + 2*b
      case IsBinary(AdditionExpr(l, op, r), Arithmentics.Times, IsConstant(data)) =>
        simplifyLoop(
          Expr.Numerical.Binary(
            left = simplifyLoop(data * l),
            operator = op,
            right = simplifyLoop(data * r)
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
      case WithAdditionFactor(f1, IsVar(a), f2, IsVar(b)) if a.id == b.id =>
        simplifyLoop((f1 + f2) * a)

      case IsBinary(IsLiteral(1), Arithmentics.Times, e) =>
        simplifyLoop(e)

      case IsBinary(a, op, b) =>
        Expr.Numerical.Binary(
          simplifyLoop(a),
          op,
          simplifyLoop(b)
        )
      case e =>
        e
    }

  }

}
