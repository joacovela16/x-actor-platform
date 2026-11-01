package xplat.di

/** The services registered under a multiple key. It stays up to date: services added or removed later show up in
  * `current` without rebuilding whoever received it.
  */
trait Many[F[_], T] {
  def current: F[List[T]]
}
