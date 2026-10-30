package xplat.extensions.persistence.jdbc

/** What changes between SQL databases. */
trait SqlDialect {
  def name: String
  def driverClass: String

  /** Column type for binary payloads. */
  def binaryType: String

  /** Column type for keys (shard names, ids, lease names). */
  def keyType: String = "VARCHAR(255)"

  /** SQL expression for the database's current time in epoch milliseconds. Lease expiry uses the database clock, so
    * nodes with skewed clocks still agree.
    */
  def nowMillis: String

  /** Suffix for a SELECT without a table, needed by MySQL-like databases. */
  def fromDual: String = ""

  /** Statements run on every new connection. */
  def connectionInit: Option[String] = None

  /** Upper bound for the pool; SQLite allows a single writer, so it uses one connection. */
  def maxPoolSize: Option[Int] = None

  /** Whether an exception means a unique key was violated (a concurrent writer won). */
  def isUniqueViolation(e: java.sql.SQLException): Boolean =
    Option(e.getSQLState).exists(_.startsWith("23"))
}

object SqlDialect {

  val Postgres: SqlDialect = new SqlDialect {
    val name = "postgres"
    val driverClass = "org.postgresql.Driver"
    val binaryType = "BYTEA"
    val nowMillis = "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)"
  }

  /** MariaDB and MySQL. */
  val MariaDb: SqlDialect = new SqlDialect {
    val name = "mariadb"
    val driverClass = "org.mariadb.jdbc.Driver"
    val binaryType = "LONGBLOB"
    val nowMillis = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)"
    override val fromDual = " FROM DUAL"
    override def isUniqueViolation(e: java.sql.SQLException): Boolean = super.isUniqueViolation(e) || e.getErrorCode == 1062
  }

  val Sqlite: SqlDialect = new SqlDialect {
    val name = "sqlite"
    val driverClass = "org.sqlite.JDBC"
    val binaryType = "BLOB"
    override val keyType = "TEXT"
    val nowMillis = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
    override val maxPoolSize: Option[Int] = Some(1)
    override def isUniqueViolation(e: java.sql.SQLException): Boolean =
      super.isUniqueViolation(e) || e.getErrorCode == 19 || Option(e.getMessage).exists(_.contains("UNIQUE constraint failed"))
  }
}
