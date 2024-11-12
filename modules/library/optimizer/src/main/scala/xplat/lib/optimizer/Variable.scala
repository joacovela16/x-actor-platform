package xplat.lib.optimizer

final case class Variable[T, Domain](
    id: String,
    kind: NumberType,
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None,
    domain: Seq[String] = Nil
) {

  def domain[Domain0](value: Domain0)(implicit ops: DomainOperation[Domain0]): Variable[T, ops.Tuple] = {
    val refs = ops.asReference(value)
    Variable(id, kind, lowerBound = lowerBound, upperBound = upperBound, domain = refs)
  }

}

object Variable {

  implicit class GetPredef[T, D, Apply](param: Variable[T, D])(implicit get: VariableGet.Aux[D, Apply]) {
    def apply(a: Apply): Expr.Numerical = get(a)
  }
  /*
  implicit class Ops[T](v: Variable[T, Unit]) {
    def &&(other: Expr[Boolean]): Expr[Boolean] = ???
    def ||(other: Expr[Boolean]): Expr[Boolean] = ???
    def ===[A](other: Expr[A]): Expr[Boolean] = ???
    def >=[A](other: Expr[A]): Expr[Boolean] = ???
    def <[A](other: Expr[A]): Expr[Boolean] = ???
    def <=[A](other: Expr[A]): Expr[Boolean] = ???
    def >[A](other: Expr[A]): Expr[Boolean] = ???
    def +[A](other: Expr[A]): Expr[A] = ???
    def -[A](other: Expr[A]): Expr[A] = ???
    def *[A](other: Expr[A])(implicit c: Combine[T, A]): Expr[c.Out] = Expr.Binary(v, other, ???)
    def /[A](other: Expr[A])(implicit c: Combine[T, A]): Expr[c.Out] = ???
  }*/

}
