package xplat.lib.optimizer

import cats.Monad
import xplat.lib.optimizer.model.Expr.Logical
import xplat.lib.optimizer.model.{Context, Model}

object Optimizer {

  def apply[F[_]](model: Model)(c: Context)(implicit M: Monad[F]): F[Unit] = {

    println(countConstraint(model, c))
    M.unit
  }

  private def countConstraint(model: Model, context: Context): Long = {
    model.constraints.map { c =>
      c.body match {
        case Logical.Forall(domain, _, _) =>
          domain.foldLeft(1L) { case (acc, item) => acc * context.getSize(item.id) }
        case Logical.Relation(_, _, _) => 1
        case Logical.Constant(_)       => 1
      }
    }.sum
  }

}
