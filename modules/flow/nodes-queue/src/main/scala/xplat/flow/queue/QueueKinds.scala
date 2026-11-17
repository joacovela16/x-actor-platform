package xplat.flow.queue

import xplat.flow.nodes.ConnectionKind
import io.circe.Json
import xplat.flow.model.{ConfigSchema, DataType}
import xplat.flow.nodes.Connections

/** The kinds of connection to a message broker, and how each one is read into the settings of its connector. They are
  * registered in `GET /api/connection-kinds` (see [[QueueKinds.all]]); the fields marked `secret` never leave the server.
  */
object QueueKinds {

  val Kafka: String = "kafka"
  val ActiveMQ: String = "activemq"
  val Redis: String = "redis"

  private def optional(name: String, description: String, secret: Boolean = false, oneOf: List[String] = Nil, default: Option[String] = None) =
    ConfigSchema.Field(
      name,
      DataType.Str,
      required = false,
      default = default.map(Json.fromString),
      secret = secret,
      oneOf = oneOf.map(Json.fromString),
      description = Some(description)
    )

  val kafka: ConnectionKind = ConnectionKind(
    Kafka,
    "Kafka",
    ConfigSchema(
      List(
        ConfigSchema.Field("bootstrapServers", DataType.Str, description = Some("host:port of one or more brokers, comma separated")),
        optional("securityProtocol", "Protocol with the brokers", oneOf = List("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"), default = Some("PLAINTEXT")),
        optional("saslMechanism", "SASL mechanism, when there is a user", oneOf = List("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"), default = Some("PLAIN")),
        optional("username", "SASL user"),
        optional("password", "SASL password", secret = true),
        ConfigSchema.Field("properties", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Other Kafka client properties, as text"))
      )
    )
  )

  val activemq: ConnectionKind = ConnectionKind(
    ActiveMQ,
    "ActiveMQ / Artemis (JMS)",
    ConfigSchema(
      List(
        ConfigSchema.Field("url", DataType.Str, description = Some("Broker URL, such as tcp://localhost:61616")),
        optional("flavor", "Which broker it is", oneOf = List("artemis", "classic"), default = Some("artemis")),
        optional("username", "User"),
        optional("password", "Password", secret = true)
      )
    )
  )

  val redis: ConnectionKind = ConnectionKind(
    Redis,
    "Redis Streams",
    ConfigSchema(
      List(
        ConfigSchema.Field("uri", DataType.Str, description = Some("Such as redis://localhost:6379")),
        optional("password", "Password, if the server needs one", secret = true)
      )
    )
  )

  val all: List[ConnectionKind] = List(kafka, activemq, redis)

  /** Kafka client properties for the connection. */
  def kafkaProperties(c: Connections.Resolved): Map[String, String] = {
    val protocol = c.text("securityProtocol").filter(_ != "PLAINTEXT").map("security.protocol" -> _)
    val sasl = for {
      user <- c.text("username")
      password <- c.secrets.get("password").filter(_.nonEmpty)
    } yield {
      val mechanism = c.text("saslMechanism").getOrElse("PLAIN")
      val module =
        if (mechanism == "PLAIN") "org.apache.kafka.common.security.plain.PlainLoginModule"
        else "org.apache.kafka.common.security.scram.ScramLoginModule"
      def quoted(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
      List("sasl.mechanism" -> mechanism, "sasl.jaas.config" -> s"$module required username=${quoted(user)} password=${quoted(password)};")
    }
    val extra = c.params.hcursor.downField("properties").focus.flatMap(_.asObject).fold(Map.empty[String, String])(_.toMap.collect {
      case (k, v) if v.isString || v.isNumber || v.isBoolean => k -> v.asString.getOrElse(v.noSpaces)
    })
    extra ++ protocol ++ sasl.getOrElse(Nil)
  }

  /** The URI of the Redis server, with the password in it when there is one. */
  def redisUri(c: Connections.Resolved): Either[String, String] =
    c.text("uri").toRight(s"connection '${c.name}' has no uri").map { uri =>
      c.secrets.get("password").filter(_.nonEmpty) match {
        case Some(password) if !uri.contains("@") =>
          val encoded = java.net.URLEncoder.encode(password, "UTF-8")
          uri.replaceFirst("^(rediss?://)", "$1:" + java.util.regex.Matcher.quoteReplacement(encoded) + "@")
        case _ => uri
      }
    }
}
