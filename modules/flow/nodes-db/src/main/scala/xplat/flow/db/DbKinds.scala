package xplat.flow.db

import xplat.flow.nodes.ConnectionKind
import io.circe.Json
import xplat.flow.model.{ConfigSchema, DataType}
import xplat.flow.nodes.Connections

/** The kinds of connection to a database, and how each one becomes a JDBC url. They are registered in `GET /api/connection-kinds`
  * (see [[DbKinds.all]]); the password never leaves the server, and it is never part of the url.
  */
object DbKinds {

  val Postgres: String = "postgres"
  val MariaDb: String = "mariadb"
  val Sqlite: String = "sqlite"

  private def server(kind: String, label: String, defaultPort: Int, sslNote: String): ConnectionKind =
    ConnectionKind(
      kind,
      label,
      ConfigSchema(
        List(
          ConfigSchema.Field("host", DataType.Str, description = Some("Host name or address")),
          ConfigSchema.Field("port", DataType.Num, required = false, default = Some(Json.fromInt(defaultPort))),
          ConfigSchema.Field("database", DataType.Str, description = Some("Name of the database")),
          ConfigSchema.Field("username", DataType.Str, required = false),
          ConfigSchema.Field("password", DataType.Str, required = false, secret = true),
          ConfigSchema.Field("ssl", DataType.Bool, required = false, default = Some(Json.False), description = Some(sslNote)),
          ConfigSchema.Field("maxPoolSize", DataType.Num, required = false, default = Some(Json.fromInt(5)), description = Some("Most connections open at once"))
        )
      )
    )

  val postgres: ConnectionKind = server(Postgres, "PostgreSQL", 5432, "Require an encrypted connection (sslmode=require)")
  val mariadb: ConnectionKind = server(MariaDb, "MariaDB / MySQL", 3306, "Encrypt the connection, without checking the certificate of the server")

  val sqlite: ConnectionKind = ConnectionKind(
    Sqlite,
    "SQLite",
    ConfigSchema(List(ConfigSchema.Field("file", DataType.Str, description = Some("Path of the database file; it is created if it does not exist"))))
  )

  val all: List[ConnectionKind] = List(postgres, mariadb, sqlite)

  /** What it takes to open a pool. `password` is kept apart from the url on purpose. */
  final case class Jdbc(driver: String, url: String, user: Option[String], password: Option[String], maxPoolSize: Int, kind: String)

  def jdbc(c: Connections.Resolved): Either[String, Jdbc] = {
    def text(f: String): Either[String, String] = c.text(f).toRight(s"connection '${c.name}' has no $f")
    val cursor = c.params.hcursor
    val poolSize = cursor.get[Int]("maxPoolSize").toOption.filter(_ > 0).getOrElse(5)
    val ssl = cursor.get[Boolean]("ssl").getOrElse(false)
    val password = c.secrets.get("password").filter(_.nonEmpty)
    def port(default: Int) = cursor.get[Int]("port").toOption.filter(p => p > 0 && p < 65536).getOrElse(default)
    c.kind match {
      case Postgres =>
        for { host <- text("host"); db <- text("database") } yield Jdbc(
          "org.postgresql.Driver",
          s"jdbc:postgresql://$host:${port(5432)}/$db${if (ssl) "?sslmode=require" else ""}",
          c.text("username"),
          password,
          poolSize,
          Postgres
        )
      case MariaDb =>
        for { host <- text("host"); db <- text("database") } yield Jdbc(
          "org.mariadb.jdbc.Driver",
          s"jdbc:mariadb://$host:${port(3306)}/$db${if (ssl) "?sslMode=trust" else ""}",
          c.text("username"),
          password,
          poolSize,
          MariaDb
        )
      // SQLite allows one writer at a time: one connection, so statements wait for each other instead of failing as locked.
      case Sqlite => text("file").map(f => Jdbc("org.sqlite.JDBC", s"jdbc:sqlite:$f?busy_timeout=5000", None, None, 1, Sqlite))
      case other  => Left(s"connection '${c.name}' is of kind '$other', which is not a database")
    }
  }
}
