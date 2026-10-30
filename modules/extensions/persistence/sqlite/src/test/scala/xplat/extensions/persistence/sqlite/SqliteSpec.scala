package xplat.extensions.persistence.sqlite

import cats.effect.{IO, Resource}
import xplat.core.io.coordination.LeaseStore
import xplat.core.persistence.api.JournalBuilder
import xplat.extensions.persistence.testkit.{JournalContract, LeaseStoreContract}

import java.nio.file.Files

object SqliteSpec {
  /** One database file for the whole run, so reconnecting finds the same data. */
  lazy val file: java.nio.file.Path = Files.createTempFile("xplat-journal", ".db")
}

class SqliteJournalSpec extends JournalContract {
  def journals: Resource[IO, JournalBuilder[IO]] = SqlitePersistence.resource[IO](SqliteSpec.file).map(_.journalBuilder)
}

class SqliteLeaseStoreSpec extends LeaseStoreContract {
  def stores: Resource[IO, LeaseStore[IO]] = SqlitePersistence.resource[IO](SqliteSpec.file).map(_.leaseStore)
}
