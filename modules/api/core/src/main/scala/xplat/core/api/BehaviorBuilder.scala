package xplat.core.api

trait BehaviorBuilder[F[_], T] {

  def apply(behaviors: Behaviors[F, T]): Behavior[F, T] = entrypoint(behaviors)

  def entrypoint(behaviors: Behaviors[F, T]): Behavior[F, T]
}
