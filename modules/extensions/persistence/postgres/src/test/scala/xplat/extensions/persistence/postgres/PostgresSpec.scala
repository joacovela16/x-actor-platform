package xplat.extensions.persistence.postgres

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import org.scalatest.BeforeAndAfterAll
import xplat.core.io.coordination.LeaseStore
import xplat.core.persistence.api.JournalBuilder
import xplat.extensions.persistence.jdbc.JdbcConfig
import xplat.extensions.persistence.testkit.{Containers, JournalContract, LeaseStoreContract}

/** One PostgreSQL container shared by the suites of this module. */
object PostgresSpec {
  private lazy val started: Option[(Containers.Container, IO[Unit])] =
    if (!Containers.available) None
    else
      Some(
        Containers
          .container("docker.io/library/postgres:16-alpine", 5432, Map("POSTGRES_PASSWORD" -> "xplat", "POSTGRES_DB" -> "xplat")) { c =>
            PostgresPersistence.resource[IO](config(c)).use(_ => IO.unit)
          }
          .allocated
          .unsafeRunSync()
      )

  private def config(c: Containers.Container): JdbcConfig =
    JdbcConfig(s"jdbc:postgresql://${c.host}:${c.port}/xplat", "postgres", "xplat", maxPoolSize = 4)

  def available: Boolean = started.isDefined
  def config: JdbcConfig = config(started.get._1)

  sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
}

class PostgresJournalSpec extends JournalContract {
  override def available: Boolean = PostgresSpec.available
  def journals: Resource[IO, JournalBuilder[IO]] = PostgresPersistence.resource[IO](PostgresSpec.config).map(_.journalBuilder)
}

class PostgresLeaseStoreSpec extends LeaseStoreContract {
  override def available: Boolean = PostgresSpec.available
  def stores: Resource[IO, LeaseStore[IO]] = PostgresPersistence.resource[IO](PostgresSpec.config).map(_.leaseStore)
}
