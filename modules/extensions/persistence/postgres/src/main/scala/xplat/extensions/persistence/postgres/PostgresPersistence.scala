package xplat.extensions.persistence.postgres

import cats.effect.kernel.{Async, Resource}
import xplat.extensions.persistence.jdbc.{JdbcConfig, JdbcPersistence, SqlDialect}

/** Journal and lease store in PostgreSQL. */
object PostgresPersistence {
  def resource[F[_]: Async](config: JdbcConfig): Resource[F, JdbcPersistence[F]] = JdbcPersistence.resource[F](config, SqlDialect.Postgres)
}
