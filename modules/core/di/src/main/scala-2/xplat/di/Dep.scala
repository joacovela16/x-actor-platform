package xplat.di

/** What a service receives from a dependency key `D`: the service itself, or a live [[Many]]. */
object Dep {
  type Of[D <: Key[_], F[_]] = D#Value[F]
}
