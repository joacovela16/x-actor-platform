package xplat.lib.optimizer

import scala.language.implicitConversions

sealed trait Expr

object Expr {
  sealed trait Declaration extends Expr

  object Declaration {

    final case class VarDecl(
        id: String,
        kind: NumberType,
        lowerBound: Option[Double] = None,
        upperBound: Option[Double] = None,
        domain: Seq[String] = Nil
    ) extends Expr.Declaration

    final case class ParamDecl(
        id: String,
        kind: NumberType,
        domain: Seq[String] = Nil,
        transform: Option[Context => Expr.Numerical] = None
    ) extends Expr.Declaration

  }

  sealed trait Statement extends Expr

  object Statement {
    final case class If[T](cond: Expr.Logical, thenBody: Expr.Numerical, elseIf: Seq[(Expr.Logical, Expr.Numerical)], elseBody: Expr.Numerical) extends Expr.Statement
    final case class Switch[R, T](access: Expr.Statement, cases: Seq[(Expr.Logical, Expr.Numerical)], defaultBody: Expr.Numerical) extends Expr.Statement
  }

  sealed trait Numerical extends Expr {
    def +(other: Numerical): Numerical = Expr.Numerical.Binary(this, other, BinaryOperator.Plus)
    def *(other: Numerical): Numerical = Expr.Numerical.Binary(this, other, BinaryOperator.Times)
    def *(other: Double): Numerical = Expr.Numerical.Binary(this, other, BinaryOperator.Times)
    def -(other: Numerical): Numerical = Expr.Numerical.Binary(this, other, BinaryOperator.Minus)
    def /(other: Numerical): Numerical = Expr.Numerical.Binary(this, other, BinaryOperator.Divide)
    def <=(other: Numerical): Logical = Expr.Logical.Relation(this, other, BinaryRelation.LessEqual)
    def >=(other: Numerical): Logical = Expr.Logical.Relation(this, other, BinaryRelation.GreatEqual)
    def >(other: Numerical): Logical = Expr.Logical.Relation(this, other, BinaryRelation.Great)
    def <(other: Numerical): Logical = Expr.Logical.Relation(this, other, BinaryRelation.Less)
  }

  object Numerical {
    final case class Binary(left: Expr.Numerical, right: Expr.Numerical, operator: BinaryOperator) extends Expr.Numerical
    final case class Sum(domain: Seq[String], condition: Context => Expr.Logical, body: Context => Expr.Numerical) extends Expr.Numerical

    final case class GetParam(id: String, references: Seq[Datum], decl: Expr.Declaration) extends Expr.Numerical
    final case class GetVar(id: String, references: Seq[Datum], decl: Expr.Declaration) extends Expr.Numerical
    final case class Constant(data: Double) extends Expr.Numerical

    implicit def mk(d: Double): Expr.Numerical = Expr.Numerical.Constant(d)
//    implicit def mk(d: Int): Expr.Numerical = Expr.Numerical.Constant(d)
    implicit def mk(d: Boolean): Expr.Numerical = Expr.Numerical.Constant(if (d) 1 else 0)

  }

  sealed trait Logical extends Expr

  object Logical {
    final case class Forall(domain: Seq[String], condition: Context => Expr.Logical, body: Context => Expr.Logical) extends Expr.Logical
    final case class Binary(left: Expr.Logical, right: Expr.Logical, operator: BinaryLogicalOperator) extends Expr.Logical
    final case class Relation(left: Expr.Numerical, right: Expr.Numerical, operator: BinaryRelation) extends Expr.Logical
    final case class Unary(expr: Expr.Logical) extends Expr.Logical
    final case class Constant(data: Boolean) extends Expr.Logical

    implicit def mk(v: Boolean): Expr.Logical = Expr.Logical.Constant(v)
  }

}
