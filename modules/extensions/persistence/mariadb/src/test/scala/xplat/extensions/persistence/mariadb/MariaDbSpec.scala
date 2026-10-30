package xplat.extensions.persistence.mariadb

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import xplat.core.io.coordination.LeaseStore
import xplat.core.persistence.api.JournalBuilder
import xplat.extensions.persistence.jdbc.JdbcConfig
import xplat.extensions.persistence.testkit.{Containers, JournalContract, LeaseStoreContract}

/** One MariaDB container shared by the suites of this module. */
object MariaDbSpec {
  private lazy val started: Option[(Containers.Container, IO[Unit])] =
    if (!Containers.available) None
    else
      Some(
        Containers
          .container("docker.io/library/mariadb:11", 3306, Map("MARIADB_ROOT_PASSWORD" -> "xplat", "MARIADB_DATABASE" -> "xplat")) { c =>
            MariaDbPersistence.resource[IO](config(c)).use(_ => IO.unit)
          }
          .allocated
          .unsafeRunSync()
      )

  private def config(c: Containers.Container): JdbcConfig =
    JdbcConfig(s"jdbc:mariadb://${c.host}:${c.port}/xplat", "root", "xplat", maxPoolSize = 4)

  def available: Boolean = started.isDefined
  def config: JdbcConfig = config(started.get._1)

  sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
}

class MariaDbJournalSpec extends JournalContract {
  override def available: Boolean = MariaDbSpec.available
  def journals: Resource[IO, JournalBuilder[IO]] = MariaDbPersistence.resource[IO](MariaDbSpec.config).map(_.journalBuilder)
}

class MariaDbLeaseStoreSpec extends LeaseStoreContract {
  override def available: Boolean = MariaDbSpec.available
  def stores: Resource[IO, LeaseStore[IO]] = MariaDbPersistence.resource[IO](MariaDbSpec.config).map(_.leaseStore)
}
