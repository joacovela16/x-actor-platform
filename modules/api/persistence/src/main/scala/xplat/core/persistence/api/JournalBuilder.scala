package xplat.core.persistence.api

trait JournalBuilder[F[_]] {
  def build(): F[Journal[F]]
}
object JournalBuilder {
  def apply[F[_]](implicit JB: JournalBuilder[F]): JournalBuilder[F] = JB
}
