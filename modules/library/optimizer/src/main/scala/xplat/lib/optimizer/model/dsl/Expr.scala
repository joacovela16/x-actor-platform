package xplat.lib.optimizer.model.dsl

import scala.language.implicitConversions

sealed trait Expr

object Expr {

  sealed trait Declaration extends Expr {
    def id: String
    def domain: Seq[SetDecl]
  }

  object Declaration {

    final case class ArgDecl(decl: Arg.Ref) extends Expr.Declaration {
      override def id: String = decl.referenceTo

      override def domain: Seq[SetDecl] = Nil
    }

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
    final case class If(cond: Expr.Predicate, thenBody: Expr.Numerical, elseIf: Seq[(Expr.Predicate, Expr.Numerical)], elseBody: Expr.Numerical) extends Expr.Statement
    final case class Switch(access: Expr.Statement, cases: Seq[(Expr.Predicate, Expr.Numerical)], defaultBody: Expr.Numerical) extends Expr.Statement
  }

  sealed trait Numerical extends Expr {
    def +(other: Numerical): Numerical = Expr.Numerical.Binary(this, Arithmentics.Plus, other)
    def *(other: Numerical): Numerical = Expr.Numerical.Binary(this, Arithmentics.Times, other)
    def -(other: Numerical): Numerical = Expr.Numerical.Binary(this, Arithmentics.Minus, other)
    def /(other: Numerical): Numerical = Expr.Numerical.Binary(this, Arithmentics.Divide, other)
    def <=(other: Numerical): Predicate.Relation = Expr.Predicate.Relation(this, Relations.LessEqual, other)
    def >=(other: Numerical): Predicate.Relation = Expr.Predicate.Relation(this, Relations.GreatEqual, other)
    def ===(other: Numerical): Predicate.Relation = Expr.Predicate.Relation(this, Relations.Equals, other)
    def >(other: Numerical): Predicate.Relation = Expr.Predicate.Relation(this, Relations.Great, other)
    def <(other: Numerical): Predicate.Relation = Expr.Predicate.Relation(this, Relations.Less, other)
    def isStatic: Boolean = false
    def isVariable: Boolean = false
  }

  object Numerical {
    def apply(n: Numerical): Numerical = n
    final case class Binary(left: Numerical, operator: Arithmentics, right: Numerical) extends Expr.Numerical
    final case class Summatory(domain: Seq[SetDecl], condition: Expr.Predicate.Logical, body: Expr.Numerical) extends Expr.Numerical

    final case class GetVar(references: Seq[Arg.Ref], over: Expr.Declaration) extends Expr.Numerical {
      def id: String = references match{
        case Nil =>
          over.id
        case xs =>
          s"${over.id}_${xs.map(_.referenceTo).mkString}"
      }
      override def isVariable: Boolean = true
    }

    final case class GetParam(references: Seq[Arg.Ref], over: Expr.Declaration) extends Expr.Numerical {
      override def isStatic: Boolean = true
    }

    final case class Constant(data: Double) extends Expr.Numerical {
      override def isStatic: Boolean = true
    }

    implicit def mk(d: Double): Expr.Numerical = Expr.Numerical.Constant(d)
    implicit def mk(d: Int): Expr.Numerical = Expr.Numerical.Constant(d)
    implicit def mk(d: Boolean): Expr.Numerical = Expr.Numerical.Constant(if (d) 1 else 0)

  }

  sealed trait Predicate extends Expr

  object Predicate {

    sealed trait Logical extends Predicate {}

    object Logical {
      final case class Binary(left: Logical, operator: BinaryLogicOps, right: Logical) extends Expr.Predicate.Logical
      final case class Not(body: Expr.Predicate) extends Expr.Predicate.Logical
      final case class Constant(data: Boolean) extends Expr.Predicate.Logical
      implicit def mk(v: Boolean): Logical = Constant(v)
    }

    final case class Relation(left: Numerical, operator: Relations, right: Numerical) extends Expr.Predicate

  }

  sealed trait Constraint extends Expr

  object Constraint {
    final case class Forall(domain: Seq[SetDecl], condition: Expr.Predicate.Logical, body: Expr.Predicate.Relation) extends Expr.Constraint
    final case class Relation(body: Expr.Predicate.Relation) extends Expr.Constraint

    implicit def mk(v: Expr.Predicate.Relation): Expr.Constraint = Constraint.Relation(v)

  }

}
