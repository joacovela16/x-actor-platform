package xplat.lib.optimizer.model

import scala.language.implicitConversions

sealed trait Expr

object Expr {

  final case class Ref(index: Int)

  sealed trait Declaration extends Expr

  object Declaration {

    final case class VarDecl(
        id: String,
        kind: NumberType,
        lowerBound: Option[Double] = None,
        upperBound: Option[Double] = None,
        domain: Seq[SetDecl] = Nil
    ) extends Expr.Declaration

    final case class ParamDecl(
        id: String,
        kind: NumberType,
        domain: Seq[SetDecl] = Nil,
        transform: Option[Expr.Numerical] = None
    ) extends Expr.Declaration

  }

  sealed trait Statement extends Expr

  object Statement {
    final case class If(cond: Expr.Logical, thenBody: Expr.Numerical, elseIf: Seq[(Expr.Logical, Expr.Numerical)], elseBody: Expr.Numerical) extends Expr.Statement
    final case class Switch(access: Expr.Statement, cases: Seq[(Expr.Logical, Expr.Numerical)], defaultBody: Expr.Numerical) extends Expr.Statement
  }

  sealed trait Numerical extends Expr {
    def +(other: Numerical): Numerical = Expr.Numerical.Binary(this, BinaryOperator.Plus, other)
    def *(other: Numerical): Numerical = Expr.Numerical.Binary(this, BinaryOperator.Times, other)
    def *(other: Double): Numerical = Expr.Numerical.Binary(this, BinaryOperator.Times, other)
    def -(other: Numerical): Numerical = Expr.Numerical.Binary(this, BinaryOperator.Minus, other)
    def /(other: Numerical): Numerical = Expr.Numerical.Binary(this, BinaryOperator.Divide, other)
    def <=(other: Numerical): Logical = Expr.Logical.Relation(this, BinaryRelation.LessEqual, other)
    def >=(other: Numerical): Logical = Expr.Logical.Relation(this, BinaryRelation.GreatEqual, other)
    def >(other: Numerical): Logical = Expr.Logical.Relation(this, BinaryRelation.Great, other)
    def <(other: Numerical): Logical = Expr.Logical.Relation(this, BinaryRelation.Less, other)
  }

  object Numerical {
    final case class Binary(left: Numerical, operator: BinaryOperator, right: Numerical) extends Expr.Numerical
    final case class Sum(domain: Seq[SetDecl], condition: Expr.Logical, body: Expr.Numerical) extends Expr.Numerical

    final case class GetParam(references: Seq[Int], over: Expr.Declaration) extends Expr.Numerical
    final case class GetVar(references: Seq[Int], over: Expr.Declaration) extends Expr.Numerical
    final case class Constant(data: Double) extends Expr.Numerical

    implicit def mk(d: Double): Expr.Numerical = Expr.Numerical.Constant(d)
    implicit def mk(d: Int): Expr.Numerical = Expr.Numerical.Constant(d)
    implicit def mk(d: Boolean): Expr.Numerical = Expr.Numerical.Constant(if (d) 1 else 0)

  }

  sealed trait Logical extends Expr

  object Logical {
    final case class Forall(domain: Seq[SetDecl], condition: Expr.Logical, body: Expr.Logical) extends Expr.Logical
    final case class Relation(left: Numerical, operator: BinaryRelation, right: Numerical) extends Expr.Logical
    final case class Constant(data: Boolean) extends Expr.Logical

    implicit def mk(v: Boolean): Expr.Logical = Expr.Logical.Constant(v)
  }

}
