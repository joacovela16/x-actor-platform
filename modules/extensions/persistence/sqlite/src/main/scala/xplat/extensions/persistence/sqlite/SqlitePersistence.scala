package xplat.extensions.persistence.sqlite

import cats.effect.kernel.{Async, Resource}
import xplat.extensions.persistence.jdbc.{JdbcConfig, JdbcPersistence, SqlDialect}

/** Journal and lease store in a SQLite file. Good for a single process; SQLite allows one writer at a time, so the pool
  * has one connection and the database runs in WAL mode.
  */
object SqlitePersistence {

  def resource[F[_]: Async](file: java.nio.file.Path, tablePrefix: String = "xplat_"): Resource[F, JdbcPersistence[F]] =
    JdbcPersistence.resource[F](JdbcConfig(s"jdbc:sqlite:${file.toAbsolutePath}?journal_mode=WAL&busy_timeout=5000", tablePrefix = tablePrefix), SqlDialect.Sqlite)

  def resource[F[_]: Async](config: JdbcConfig): Resource[F, JdbcPersistence[F]] = JdbcPersistence.resource[F](config, SqlDialect.Sqlite)
}
