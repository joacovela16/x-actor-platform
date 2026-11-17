package xplat.flow.mongo

import xplat.flow.nodes.ConnectionKind
import com.mongodb.{ConnectionString, MongoClientSettings, MongoCredential}
import io.circe.Json
import xplat.flow.model.{ConfigSchema, DataType}
import xplat.flow.nodes.Connections

import java.util.concurrent.TimeUnit

/** The `mongodb` connection kind, and how it becomes the settings of a client. The password is a secret: it is given to the driver
  * as a credential and is never part of the connection string that is shown or logged.
  */
object MongoKinds {

  val Mongo: String = "mongodb"

  val mongodb: ConnectionKind = ConnectionKind(
    Mongo,
    "MongoDB",
    ConfigSchema(
      List(
        ConfigSchema.Field("uri", DataType.Str, required = false, description = Some("Connection string without the password (mongodb:// or mongodb+srv://); when set, host and port are not used")),
        ConfigSchema.Field("host", DataType.Str, required = false, default = Some(Json.fromString("localhost")), description = Some("Host name or address")),
        ConfigSchema.Field("port", DataType.Num, required = false, default = Some(Json.fromInt(27017))),
        ConfigSchema.Field("database", DataType.Str, description = Some("Database that the nodes use")),
        ConfigSchema.Field("username", DataType.Str, required = false),
        ConfigSchema.Field("password", DataType.Str, required = false, secret = true),
        ConfigSchema.Field("authSource", DataType.Str, required = false, default = Some(Json.fromString("admin")), description = Some("Database that holds the user")),
        ConfigSchema.Field("tls", DataType.Bool, required = false, default = Some(Json.False), description = Some("Encrypt the connection")),
        ConfigSchema.Field("maxPoolSize", DataType.Num, required = false, default = Some(Json.fromInt(10)), description = Some("Most connections open at once"))
      )
    )
  )

  val all: List[ConnectionKind] = List(mongodb)

  /** What it takes to open a client: its settings and the database the nodes work on. */
  final case class Client(settings: MongoClientSettings, database: String)

  def client(c: Connections.Resolved): Either[String, Client] =
    if (c.kind != Mongo) Left(s"connection '${c.name}' is of kind '${c.kind}', which is not MongoDB")
    else {
      val cursor = c.params.hcursor
      val tls = cursor.get[Boolean]("tls").getOrElse(false)
      val poolSize = cursor.get[Int]("maxPoolSize").toOption.filter(_ > 0).getOrElse(10)
      val port = cursor.get[Int]("port").toOption.filter(p => p > 0 && p < 65536).getOrElse(27017)
      val uri = c.text("uri").getOrElse(s"mongodb://${c.text("host").getOrElse("localhost")}:$port")
      for {
        database <- c.text("database").toRight(s"connection '${c.name}' has no database")
        parsed <- scala.util.Try(new ConnectionString(uri)).toEither.left.map(e => s"connection '${c.name}': not a MongoDB connection string: ${e.getMessage}")
      } yield {
        val builder = MongoClientSettings
          .builder()
          .applyConnectionString(parsed)
          .applyToConnectionPoolSettings(b => { b.maxSize(poolSize); () })
          // A server that is down shows up as an error of the node, soon, instead of a node that waits for its whole timeout.
          .applyToClusterSettings(b => { b.serverSelectionTimeout(5, TimeUnit.SECONDS); () })
          .applyToSocketSettings(b => { b.connectTimeout(5, TimeUnit.SECONDS); () })
        if (tls) builder.applyToSslSettings(b => { b.enabled(true); () })
        c.text("username").foreach { user =>
          val password = c.secrets.get("password").getOrElse("").toCharArray
          builder.credential(MongoCredential.createCredential(user, c.text("authSource").getOrElse("admin"), password))
        }
        Client(builder.build(), database)
      }
    }
}
