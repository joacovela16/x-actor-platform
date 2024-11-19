package xplat.lib.optimizer.model.core

trait Solver[F[_]] {
  def presolve(stage: Stage): F[Stage]
  def solve(stage: Stage): F[Unit]
}
