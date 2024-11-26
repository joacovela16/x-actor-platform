package xplat.lib.optimizer.solver

import xplat.lib.optimizer.model.dsl.Expr.Logical
import xplat.lib.optimizer.model.dsl.{Arithmentics, Expr, Relations}
import xplat.lib.optimizer.unapply._

object Simplifier {

  private def partition(expr: Expr.Numerical): (Seq[Expr.Numerical], Seq[Expr.Numerical]) = {
    Arranger.flatten(Simplifier(expr)).partition {
      case IsConstant(_) =>
        false
      case _ =>
        true
    }
  }

  def apply(expr: Expr.Logical): Expr.Logical = {
    expr match {
      // a + 5 <= 7 => a <= 7 - 5
      case Expr.Logical.Relation(e1, op, e2) =>
        val (t1, c1) = partition(e1)
        val (t2, c2) = partition(e2)

        op match {
          case Relations.Equals => ???
          case Relations.Less   => ???
          case Relations.LessEqual =>
            val s1 = Simplifier(Arranger.rebuild(t1 ++ t2.map(e => -1 * e)))
            val s2 = Simplifier(Arranger.rebuild(c2 ++ c1.map(e => -1 * e)))
            Expr.Logical.Relation(s1, op, s2)
          case Relations.Great      => ???
          case Relations.GreatEqual => ???
        }
      case e =>
        e
    }
  }

  def apply(expr: Expr.Constraint): Expr.Constraint = {
    expr match {
      case Expr.Constraint.Forall(domain, condition, body) =>
        Expr.Constraint.Forall(domain, condition, Simplifier(body))
      case Expr.Constraint.Relation(body) =>
        Expr.Constraint.Relation(Simplifier(body))
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
      case IsBinary(IsConstantReducible(e1), IsFactor(op1), IsBinary(IsConstantReducible(e2), IsFactor(op2), e3)) =>
        simplifyLoop(
          Expr.Numerical.Binary(
            simplifyLoop(Expr.Numerical.Binary(e1, op1, e2)),
            op2,
            simplifyLoop(e3)
          )
        )
      // (? + y) + y => x + 2y
      case IsBinary(IsBinary(l1, IsAddition(op1), IsVarFactor(f1, v1)), IsAddition(op2), IsVarFactor(f2, v2)) if v1.id == v2.id =>
        simplifyLoop(
          Expr.Numerical.Binary(
            simplifyLoop(l1),
            op1,
            simplifyLoop((op1.withSign(f1) + op2.withSign(f2)) * v1)
          )
        )
        // ? + (-1*x) = ? - x
      case IsBinary(e1, IsAddition(op), IsBinary(IsLiteral(-1), Arithmentics.Times, e2)) =>
        simplifyLoop(Expr.Numerical.Binary(e1, if (op.sign == 1) Arithmentics.Minus else Arithmentics.Plus, e2))
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
        val result = Expr.Numerical.Binary(
          simplifyLoop(a),
          op,
          simplifyLoop(b)
        )

        result match {
          case IsBinary(IsLiteral(0), Arithmentics.Times, _) =>
            0
          case IsBinary(_, Arithmentics.Times, IsLiteral(0)) =>
            0
          case IsBinary(IsLiteral(1), Arithmentics.Times, e) =>
            e
          case IsBinary(e, Arithmentics.Times, IsLiteral(1)) =>
            simplifyLoop(e)
          case IsBinary(e, Arithmentics.Plus, IsLiteral(0)) =>
            simplifyLoop(e)
          case IsBinary(IsLiteral(0), Arithmentics.Plus, e) =>
            simplifyLoop(e)
          case IsBinary(IsConstantReducible(_), _, IsConstantReducible(_)) =>
            simplifyLoop(result)
          case IsBinary(IsVarFactor(_, v1), IsAddition(_), IsVarFactor(_, v2)) if v1.id == v2.id =>
            simplifyLoop(result)
          case _ =>
            result
        }
      case e =>
        e
    }

  }

}
