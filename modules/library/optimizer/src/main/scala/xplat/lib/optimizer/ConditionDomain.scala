package xplat.lib.optimizer

trait ConditionDomain[Domain, F[_], Out] {
  type Apply
  type Handler
  def apply(a: Apply): Handler
}

object ConditionDomain extends ConditionalDomainImpl[Expr, Boolean] {
  type Aux[Domain, F[_], Out, Apply0, Handler0] = ConditionDomain[Domain, F, Out] { type Apply = Apply0; type Handler = Handler0 }
}
