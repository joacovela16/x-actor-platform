package xplat.lib.optimizer


final case class Param[T, Domain](
                                   id: String,
                                   kind: NumberType,
                                   domain: Seq[String] = Nil,
                                   transform: Option[Context => Expr.Numerical] = None
                                 ) {

  def domain[Domain0](value: Domain0)(implicit ops: DomainOperation[Domain0]): Param[T, ops.Tuple] = {
    val ref = ops.asReference(value)
    Param(id, kind, ref, transform)
  }

}

object Param {

  implicit class MapPredef[
    T,
    D,
    Tuple,
    Apply,
    Conditional
  ](param: Param[T, Tuple])(implicit
                            ops: DomainOperation.Aux[D, Tuple, Apply, Conditional]
  ) {

    def project(a: Apply): Param[T, Tuple] = {
      Param(param.id, param.kind, param.domain, Option(ops(a)))
    }

  }

  implicit class GetPredef[T, D, Apply](param: Param[T, D])(implicit get: ParamGet.Aux[D, Apply]) {
    def apply(a: Apply): Expr.Numerical = get(a)
  }

}