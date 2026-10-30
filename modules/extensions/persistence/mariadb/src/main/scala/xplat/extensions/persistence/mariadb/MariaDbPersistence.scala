package xplat.extensions.persistence.mariadb

import cats.effect.kernel.{Async, Resource}
import xplat.extensions.persistence.jdbc.{JdbcConfig, JdbcPersistence, SqlDialect}

/** Journal and lease store in MariaDB or MySQL (`jdbc:mariadb://...` or `jdbc:mysql://...` urls). */
object MariaDbPersistence {
  def resource[F[_]: Async](config: JdbcConfig): Resource[F, JdbcPersistence[F]] = JdbcPersistence.resource[F](config, SqlDialect.MariaDb)
}
