package xplat.extensions.persistence.jdbc

import cats.effect.kernel.{Async, Resource}
import cats.implicits._
import com.zaxxer.hikari.HikariConfig
import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import xplat.core.io.coordination.LeaseStore
import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}
import xplat.core.persistence.api.{Journal, JournalBuilder}

import scala.concurrent.duration.FiniteDuration

/** @param url
  *   JDBC url of the database.
  * @param tablePrefix
  *   prefix of the tables (`<prefix>events`, `<prefix>snapshots`, `<prefix>leases`), so several systems can share a
  *   database.
  * @param createSchema
  *   creates the tables if they do not exist when the pool starts.
  */
final case class JdbcConfig(
    url: String,
    user: String = "",
    password: String = "",
    maxPoolSize: Int = 10,
    tablePrefix: String = "xplat_",
    createSchema: Boolean = true
) {
  require(tablePrefix.matches("[A-Za-z0-9_]*"), s"invalid table prefix '$tablePrefix'")
}

/** Journal and lease store on a SQL database, sharing one connection pool.
  *
  * Events are rows keyed by (shard, id, offset), so a concurrent writer of the same entity fails instead of
  * overwriting. Writes of one batch happen in one transaction.
  */
final class JdbcPersistence[F[_]] private (xa: Transactor[F], dialect: SqlDialect, prefix: String)(implicit A: Async[F]) {

  private val events = Fragment.const(s"${prefix}events")
  private val snapshots = Fragment.const(s"${prefix}snapshots")
  private val leases = Fragment.const(s"${prefix}leases")
  private val now = Fragment.const(dialect.nowMillis)

  private[jdbc] def createSchema: F[Unit] = {
    val key = dialect.keyType
    val bin = dialect.binaryType
    val statements = List(
      s"CREATE TABLE IF NOT EXISTS ${prefix}events (shard $key NOT NULL, entity_id $key NOT NULL, seq BIGINT NOT NULL, created_at BIGINT NOT NULL, data $bin NOT NULL, PRIMARY KEY (shard, entity_id, seq))",
      s"CREATE TABLE IF NOT EXISTS ${prefix}snapshots (shard $key NOT NULL, entity_id $key NOT NULL, seq BIGINT NOT NULL, created_at BIGINT NOT NULL, data $bin NOT NULL, PRIMARY KEY (shard, entity_id))",
      s"CREATE TABLE IF NOT EXISTS ${prefix}leases (name $key NOT NULL PRIMARY KEY, owner $key NOT NULL, expires_at BIGINT NOT NULL)"
    )
    statements.traverse_(s => Fragment.const(s).update.run).transact(xa)
  }

  // ---- Journal ----

  private def lastSeq(shard: Shard): ConnectionIO[Long] =
    (fr"SELECT COALESCE(MAX(seq), 0) FROM" ++ events ++ fr"WHERE shard = ${shard.name} AND entity_id = ${shard.id}").query[Long].unique

  private def snapshotSeq(shard: Shard): ConnectionIO[Long] =
    (fr"SELECT seq FROM" ++ snapshots ++ fr"WHERE shard = ${shard.name} AND entity_id = ${shard.id}").query[Long].option.map(_.getOrElse(0L))

  val journal: Journal[F] = new Journal[F] {

    def create(shard: Shard): F[Unit] = A.unit

    /** Events after the snapshot, streamed from the database. */
    def getEvents(shard: Shard): F[fs2.Stream[F, JournalEvent]] =
      snapshotSeq(shard).transact(xa).map { from =>
        (fr"SELECT seq, created_at, data FROM" ++ events ++ fr"WHERE shard = ${shard.name} AND entity_id = ${shard.id} AND seq > $from ORDER BY seq")
          .query[(Long, Long, Array[Byte])]
          .stream
          .transact(xa)
          .map { case (seq, at, data) => JournalEvent(shard.name, shard.id, seq, at, data) }
      }

    def getSnapshot(shard: Shard): F[Option[JournalSnapshot]] =
      (fr"SELECT seq, data FROM" ++ snapshots ++ fr"WHERE shard = ${shard.name} AND entity_id = ${shard.id}")
        .query[(Long, Array[Byte])]
        .option
        .transact(xa)
        .map(_.map { case (seq, data) => JournalSnapshot(shard.name, shard.id, seq, seq, Some(data)) })

    def writeSnapshot(shard: Shard, data: Array[Byte]): F[Unit] =
      A.realTime.flatMap { at =>
        (for {
          seq <- lastSeq(shard)
          _   <- (fr"DELETE FROM" ++ snapshots ++ fr"WHERE shard = ${shard.name} AND entity_id = ${shard.id}").update.run
          _ <- (fr"INSERT INTO" ++ snapshots ++ fr"(shard, entity_id, seq, created_at, data) VALUES (${shard.name}, ${shard.id}, $seq, ${at.toMillis}, $data)").update.run
        } yield ()).transact(xa)
      }

    def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): F[Unit] =
      if (entries.isEmpty) A.unit
      else
        A.realTime.flatMap { at =>
          (for {
            last <- lastSeq(shard)
            rows = entries.zipWithIndex.map { case (data, i) => (shard.name, shard.id, last + 1 + i, at.toMillis, data) }
            _ <- Update[(String, String, Long, Long, Array[Byte])](s"INSERT INTO ${prefix}events (shard, entity_id, seq, created_at, data) VALUES (?, ?, ?, ?, ?)").updateMany(rows.toList)
          } yield ()).transact(xa)
        }

    /** The pool is shared by every entity and closed with the resource that created it. */
    def close(): F[Unit] = A.unit
  }

  val journalBuilder: JournalBuilder[F] = () => A.pure(journal)

  // ---- Leases ----

  val leaseStore: LeaseStore[F] = new LeaseStore[F] {

    def tryAcquire(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] = {
      val ttl = timeToLive.toMillis
      val takeOver =
        (fr"UPDATE" ++ leases ++ fr"SET owner = $owner, expires_at =" ++ now ++ fr"+ $ttl WHERE name = $name AND (owner = $owner OR expires_at <=" ++ now ++ fr")").update.run
      val insert =
        (fr"INSERT INTO" ++ leases ++ fr"(name, owner, expires_at) SELECT $name, $owner," ++ now ++ fr"+ $ttl" ++ Fragment.const(dialect.fromDual) ++
          fr"WHERE NOT EXISTS (SELECT 1 FROM" ++ leases ++ fr"WHERE name = $name)").update.run
      takeOver.transact(xa).flatMap {
        case 1 => A.pure(true)
        case _ =>
          insert.transact(xa).map(_ == 1).recover { case e: java.sql.SQLException if dialect.isUniqueViolation(e) => false }
      }
    }

    def renew(name: String, owner: String, timeToLive: FiniteDuration): F[Boolean] =
      (fr"UPDATE" ++ leases ++ fr"SET expires_at =" ++ now ++ fr"+ ${timeToLive.toMillis} WHERE name = $name AND owner = $owner AND expires_at >" ++ now).update.run
        .transact(xa)
        .map(_ == 1)

    def release(name: String, owner: String): F[Boolean] =
      (fr"DELETE FROM" ++ leases ++ fr"WHERE name = $name AND owner = $owner AND expires_at >" ++ now).update.run.transact(xa).map(_ == 1)

    def holder(name: String): F[Option[String]] =
      (fr"SELECT owner FROM" ++ leases ++ fr"WHERE name = $name AND expires_at >" ++ now).query[String].option.transact(xa)
  }
}

object JdbcPersistence {

  def resource[F[_]](config: JdbcConfig, dialect: SqlDialect)(implicit A: Async[F]): Resource[F, JdbcPersistence[F]] = {
    val hikari = new HikariConfig()
    hikari.setDriverClassName(dialect.driverClass)
    hikari.setJdbcUrl(config.url)
    if (config.user.nonEmpty) hikari.setUsername(config.user)
    if (config.password.nonEmpty) hikari.setPassword(config.password)
    hikari.setMaximumPoolSize(dialect.maxPoolSize.fold(config.maxPoolSize)(math.min(_, config.maxPoolSize)))
    hikari.setPoolName(s"xplat-${dialect.name}")
    dialect.connectionInit.foreach(hikari.setConnectionInitSql)

    HikariTransactor.fromHikariConfig[F](hikari).evalMap { xa =>
      val persistence = new JdbcPersistence[F](xa, dialect, config.tablePrefix)
      A.whenA(config.createSchema)(persistence.createSchema).as(persistence)
    }
  }
}
