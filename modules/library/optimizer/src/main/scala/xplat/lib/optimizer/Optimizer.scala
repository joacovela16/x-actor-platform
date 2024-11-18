package xplat.lib.optimizer

import cats.Monad
import cats.effect.MonadCancel
import cats.implicits._
import xplat.core.api._
import xplat.lib.optimizer.model.Expr.{Logical, Numerical}
import xplat.lib.optimizer.model.{Context, Expr, Model}

object Optimizer {

  sealed trait SolverCommand[F[_]] {}

  object SolverCommand {
    final case class Solve[F[_]](replyTo: ActorRef[F, Long]) extends SolverCommand[F]
  }

  def apply[F[_]](model: Model)(c: Context)(implicit M: MonadCancel[F, Throwable], asb: ActorSystemBuilder[F]): F[Unit] = {
    ActorSystem[F].use { as =>
      for {
        solver <- as.spawn("solver")(solver[F](model, c))
        count <- solver.ask[Long](x => SolverCommand.Solve(x))
        _ = println(count)
        _ <- as.terminate()
      } yield ()
    }
  }

  private def solver[F[_]: Monad](model: Model, context: Context)(b: Behaviors[F, SolverCommand[F]]): Behavior[F, SolverCommand[F]] = {

    b.message { case SolverCommand.Solve(replyTo) =>
      val c = countConstraint(model, context)

      replyTo ! c
    }
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

  private def countVariables(model: Model, context: Context):Long = {
    def countVar(expr:Expr.Numerical):Long = {
      expr match {
        case Numerical.Binary(left, operator, right) => ???
        case Numerical.Sum(domain, condition, body) => ???
        case Numerical.GetParam(references, over) => ???
        case Numerical.GetVar(references, over) => ???
        case _ => ???
      }
    }
    model.constraints.map{c =>
      c.body match {
        case Logical.Forall(domain, condition, body) => ???
        case Logical.Relation(left, operator, right) => ???
        case _ => 0
      }
    }
  }

  private def evaluate(context: Context, expr:Expr.Logical):Boolean={
    ???
  }

}
