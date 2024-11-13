package xplat.lib.optimizer

import scala.language.implicitConversions

final case class Var[T, Domain](
    id: String,
    kind: NumberType,
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None,
    domain: Seq[String] = Nil
) {

  def domain[Domain0](value: Domain0)(implicit ops: DomainOperation[Domain0]): Var[T, ops.Tuple] = {
    val refs = ops.asReference(value)
    Var(id, kind, lowerBound = lowerBound, upperBound = upperBound, domain = refs)
  }
//
//  def +(other: Expr.Numerical): Expr.Numerical = Expr.Numerical.Binary()
//  def *(other: Expr.Numerical): Expr.Numerical = ???
//  def -(other: Expr.Numerical): Expr.Numerical = ???
//  def /(other: Expr.Numerical): Expr.Numerical = ???

//  private val asGet = Expr.Numerical.GetVar(id, )

  val asDecl: Expr.Declaration = {
    Expr.Declaration.VarDecl(
      id = id,
      kind = kind,
      lowerBound = lowerBound,
      upperBound = upperBound,
      domain = domain
    )
  }

}

object Var {

  implicit class Get[T, D, Apply](v: Var[T, D])(implicit get: VariableGet.Aux[D, Apply]) {
    def apply(a: Apply): Expr.Numerical = {
      Expr.Numerical.GetVar(get(a), v.asDecl)
    }

  }

}
