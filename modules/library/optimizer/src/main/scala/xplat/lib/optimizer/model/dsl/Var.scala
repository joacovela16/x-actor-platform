package xplat.lib.optimizer.model.dsl

import com.softwaremill.quicklens.ModifyPimp

import scala.language.implicitConversions

final case class Var[T, Domain](
    id: String,
    kind: NumberType,
    domain: Seq[SetDecl] = Nil,
    criterias: Seq[VariableCriteria] = Nil
) {

  def restrictedTo(head: VariableCriteria, tail: VariableCriteria*): Var[T, Domain] = {
    this.modify(_.criterias).using(xs => xs ++ (head +: tail))
  }

  def domain[Domain0](value: Domain0)(implicit ops: DomainOperation[Domain0]): Var[T, ops.Tuple] = {
    val refs = ops.asReference(value)
    Var(id, kind, domain = refs)
  }

  val asDecl: Expr.Declaration = {
    Expr.Declaration.VarDecl(
      id = id,
      kind = kind,
      domain = domain,
      localIndexedId = None,
      criterias = Nil
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
