package xplat.di

/** What a service receives from a dependency key `D`: the service itself, or a live [[Many]]. Scala 3 forbids the
  * projection `D#Value[F]` on an abstract type, so it is a match type on the kind of key.
  */
object Dep {
  type Of[D <: Key[?], F[_]] = D match {
    case Key.Single[t]   => t
    case Key.Multiple[t] => Many[F, t]
  }
}
