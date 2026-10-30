package xplat.extensions.persistence.mongodb

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import xplat.core.io.coordination.LeaseStore
import xplat.core.persistence.api.JournalBuilder
import xplat.extensions.persistence.testkit.{Containers, JournalContract, LeaseStoreContract}

/** One MongoDB container shared by the suites of this module. */
object MongoSpec {
  private lazy val started: Option[(Containers.Container, IO[Unit])] =
    if (!Containers.available) None
    else
      Some(
        Containers
          .container("docker.io/library/mongo:7", 27017)(c => MongoPersistence.resource[IO](config(c)).use(_.serverTime.void))
          .allocated
          .unsafeRunSync()
      )

  private def config(c: Containers.Container): MongoConfig =
    MongoConfig(s"mongodb://${c.host}:${c.port}/?serverSelectionTimeoutMS=2000", "xplat")

  def available: Boolean = started.isDefined
  def config: MongoConfig = config(started.get._1)

  sys.addShutdownHook(started.foreach(_._2.unsafeRunSync()))
}

class MongoJournalSpec extends JournalContract {
  override def available: Boolean = MongoSpec.available
  def journals: Resource[IO, JournalBuilder[IO]] = MongoPersistence.resource[IO](MongoSpec.config).map(_.journalBuilder)
}

class MongoLeaseStoreSpec extends LeaseStoreContract {
  override def available: Boolean = MongoSpec.available
  def stores: Resource[IO, LeaseStore[IO]] = MongoPersistence.resource[IO](MongoSpec.config).map(_.leaseStore)
}
