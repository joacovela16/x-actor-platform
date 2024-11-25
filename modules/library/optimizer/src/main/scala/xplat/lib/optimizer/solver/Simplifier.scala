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
    simplifyLoop(
      Arranger(expr)
    )
  }

  private def simplifyLoop(expr: Expr.Numerical): Expr.Numerical = {
    expr match {
      // ************** BASE **************//
      case IsConstant(e) =>
        e
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
      // ************** rearrange **************//
      // moves variables to left
//      case WithAdditionFactor(f1, IsConstant(a), f2, IsVar(b)) =>
//        simplifyLoop(
//          f2 * b + f1 * a
//        )
//      case WithAdditionFactor(f1, IsNotVar(e1), f2, IsVar(v1)) =>
//        simplifyLoop(f2 * v1 + f1 * e1)
      // (x + y) + y => x + 2y
     /* case IsBinary(IsBinary(l1, Arithmentics.Plus, IsVarFactor(f1, v1)), Arithmentics.Plus, IsVarFactor(f2, v2)) if v1.id == v2.id =>
        simplifyLoop(
          Expr.Numerical.Binary(
            simplifyLoop(l1),
            Arithmentics.Plus,
            simplifyLoop((f1 + f2) * v1)
          )
        )*/
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
      // b +/- a => -/+ a + b
      case WithAdditionFactor(f1, IsVar(b), f2, IsVar(a)) if a.id < b.id =>
        simplifyLoop {
          f2 * b + f1 * b
        }
      // x3 + (x2 + x1) => x1 + x2 + x3
      // 2*x3 + (x2 + x1) => x1 + x2 + 2*x3
      case WithAdditionFactor(f1, IsVar(v1), f2, WithAdditionFactor(f3, e, f4, IsVar(v3))) if v3.id < v1.id =>
        simplifyLoop(
          f4 * f2 * f3 + f1 * v1 + f2 * f3 * e
        )
      // (2x + y) + x => (2x + x) + y
      case WithAdditionFactor(f1, WithAdditionFactor(f2, IsVar(v1), f3, e), f4, IsVar(v3)) if v3.id < v1.id =>
        simplifyLoop(f4 * v3) + simplifyLoop(f1 * (f3 * e + f2 * v1))

      // 2x + 2y + x => 3x + 2y
      //      case WithAdditionFactor(f1, IsVar(v1), f4, WithAdditionFactor(f2, IsVar(v2), f3, IsVar(v3))) if v1.id == v3.id =>
      //        simplifyLoop(
      //          ((f1 + f4 * f3) * v1) + (f4 * f2 * v2)
      //        )

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
