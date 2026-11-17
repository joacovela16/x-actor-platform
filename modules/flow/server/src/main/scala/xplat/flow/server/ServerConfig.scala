package xplat.flow.server

import java.nio.file.{Path, Paths}
import scala.concurrent.duration._

/** How the standalone server runs. Each setting comes from an environment variable or an argument, and the argument wins.
  *
  * @param dataDir
  *   where it keeps what has to survive a restart: the flows, the triggers, the connections and their secrets and, with
  *   [[ServerConfig.Database.Sqlite]], the database of the instances.
  * @param nodeTimeout
  *   how long a node may run before it fails.
  * @param mongo
  *   where the server keeps everything with [[ServerConfig.Database.Mongo]].
  * @param examples
  *   whether the example flows are installed when the server starts with no flows (see [[Examples]]).
  * @param pool
  *   how long the clients of connections (MongoDB, JDBC, publishers) live, and how many there may be (see `ClientPool`).
  * @param aiCassettes
  *   whether the answers of the AI services are recorded to files or played back instead of calling them (for tests).
  * @param pluginsDir
  *   where the plugins are, one per subdirectory; `<dataDir>/plugins` when not given (see [[pluginsDirectory]]).
  * @param pluginsStrict
  *   a plugin that cannot be loaded stops the start, instead of being reported and left out.
  * @param scalaNodes
  *   offers `script.scala` (on by default), whose Scala code runs in the server's JVM without a sandbox; turn it off where flows are written by whoever may not run code in the server.
  * @param otel
  *   where traces are sent by OTLP (HTTP), and under which service; none, no traces.
  */
final case class ServerConfig(
    host: String = "0.0.0.0",
    port: Int = 8080,
    dataDir: Path = Paths.get("flow-data"),
    database: ServerConfig.Database = ServerConfig.Database.Sqlite,
    nodeTimeout: FiniteDuration = 5.minutes,
    aiCassettes: ServerConfig.AiCassettes = ServerConfig.AiCassettes.Off,
    mongo: ServerConfig.Mongo = ServerConfig.Mongo(),
    examples: Boolean = true,
    pool: xplat.flow.nodes.ClientPool.Config = xplat.flow.nodes.ClientPool.Config(),
    pluginsDir: Option[Path] = None,
    pluginsStrict: Boolean = false,
    scalaNodes: Boolean = true,
    otel: Option[ServerConfig.Otel] = None
) {
  def pluginsDirectory: Path = pluginsDir.getOrElse(dataDir.resolve("plugins"))
}

object ServerConfig {

  /** @param endpoint the OTLP/HTTP collector (`http://collector:4318`); traces go to `<endpoint>/v1/traces`. */
  final case class Otel(endpoint: String, service: String = "x-flow")

  sealed trait Database
  object Database {

    /** The instances are kept in a SQLite file in the data directory. */
    case object Sqlite extends Database

    /** Nothing is kept: neither the instances nor the flows. For trying things out. */
    case object Memory extends Database

    /** Everything is kept in MongoDB (see [[ServerConfig.Mongo]]): the instances, the flows, the triggers, the connections and
      * their secrets. Nothing goes to the data directory.
      */
    case object Mongo extends Database
  }

  /** @param uri
    *   connection string, with the user and password if the server needs them: `mongodb://user:pass@host:27017`.
    * @param database
    *   the database; its collections start with `flow_`.
    */
  final case class Mongo(uri: String = "mongodb://localhost:27017", database: String = "flow")

  /** What to do with the answers of the AI services (see `xplat.flow.ai.Cassettes`). */
  sealed trait AiCassettes
  object AiCassettes {
    case object Off extends AiCassettes
    final case class Record(directory: Path) extends AiCassettes
    final case class Replay(directory: Path) extends AiCassettes
  }

  val usage: String =
    """flow server
      |
      |  --host <host>                  address to listen on                 (FLOW_HOST, default 0.0.0.0)
      |  --port <port>                  port to listen on                    (FLOW_PORT, default 8080)
      |  --data-dir <dir>               directory for flows, secrets, db     (FLOW_DATA_DIR, default ./flow-data)
      |  --db <sqlite|mongodb|memory>   where everything is kept; memory keeps nothing  (FLOW_DB, default sqlite)
      |  --mongo-uri <uri>              MongoDB connection string with --db mongodb      (FLOW_MONGO_URI, default mongodb://localhost:27017)
      |  --mongo-database <name>        MongoDB database with --db mongodb               (FLOW_MONGO_DATABASE, default flow)
      |  --examples <true|false>         install the example flows on a first start  (FLOW_EXAMPLES, default true)
      |  --node-timeout-seconds <n>     how long a node may run              (FLOW_NODE_TIMEOUT_SECONDS, default 300)
      |  --pool-idle-seconds <n>        close a client of a connection unused this long   (FLOW_POOL_IDLE_SECONDS, default 600)
      |  --pool-max <n>                 most clients of connections open at once          (FLOW_POOL_MAX, default 50)
      |  --pool-acquire-seconds <n>     how long a node waits when all are in use         (FLOW_POOL_ACQUIRE_SECONDS, default 30)
      |  --pool-shutdown-seconds <n>    how long a stop waits for running operations      (FLOW_POOL_SHUTDOWN_SECONDS, default 15)
      |  --ai-record <dir>              keep the answers of the AI services in this directory    (FLOW_AI_RECORD)
      |  --ai-replay <dir>              answer AI nodes from this directory, never calling the service  (FLOW_AI_REPLAY)
      |  --plugins-dir <dir>             where the plugins are, one per directory  (FLOW_PLUGINS_DIR, default <data-dir>/plugins)
      |  --plugins-strict <true|false>  a plugin that cannot be loaded stops the start  (FLOW_PLUGINS_STRICT, default false)
      |  --scala-nodes <true|false>     offer script.scala: Scala code that runs in the server, NOT sandboxed  (FLOW_SCALA_NODES, default true)
      |  --otel-endpoint <url>          send traces by OTLP/HTTP to this collector, e.g. http://localhost:4318  (OTEL_EXPORTER_OTLP_ENDPOINT; none: no traces)
      |  --otel-service <name>          the service name of the traces  (OTEL_SERVICE_NAME, default x-flow)
      |  --help
      |""".stripMargin

  /** Reads the configuration from the environment and then the arguments (`--name value`). */
  def parse(args: List[String], env: Map[String, String]): Either[String, ServerConfig] = {
    val fromEnv = List("host" -> "FLOW_HOST", "port" -> "FLOW_PORT", "data-dir" -> "FLOW_DATA_DIR", "db" -> "FLOW_DB", "node-timeout-seconds" -> "FLOW_NODE_TIMEOUT_SECONDS", "ai-record" -> "FLOW_AI_RECORD", "ai-replay" -> "FLOW_AI_REPLAY", "mongo-uri" -> "FLOW_MONGO_URI", "mongo-database" -> "FLOW_MONGO_DATABASE", "examples" -> "FLOW_EXAMPLES", "pool-idle-seconds" -> "FLOW_POOL_IDLE_SECONDS", "pool-max" -> "FLOW_POOL_MAX", "pool-acquire-seconds" -> "FLOW_POOL_ACQUIRE_SECONDS", "pool-shutdown-seconds" -> "FLOW_POOL_SHUTDOWN_SECONDS", "plugins-dir" -> "FLOW_PLUGINS_DIR", "plugins-strict" -> "FLOW_PLUGINS_STRICT", "scala-nodes" -> "FLOW_SCALA_NODES", "otel-endpoint" -> "OTEL_EXPORTER_OTLP_ENDPOINT", "otel-service" -> "OTEL_SERVICE_NAME")
      .flatMap { case (name, variable) => env.get(variable).filter(_.nonEmpty).map(name -> _) }

    def seconds(value: String): Either[String, FiniteDuration] = value.toLongOption.filter(_ > 0).map(_.seconds).toRight(s"not a number of seconds: '$value'")

    def pairs(rest: List[String]): Either[String, List[(String, String)]] = rest match {
      case Nil                                         => Right(Nil)
      case name :: value :: tail if name.startsWith("--") => pairs(tail).map((name.drop(2) -> value) :: _)
      case other :: _                                  => Left(s"cannot understand '$other'")
    }

    // A service name without an endpoint does not turn traces on.
    def endpointOnly(c: ServerConfig): ServerConfig = c.copy(otel = c.otel.filter(_.endpoint.nonEmpty))

    pairs(args).flatMap { fromArgs =>
      (fromEnv ++ fromArgs).foldLeft[Either[String, ServerConfig]](Right(ServerConfig())) { case (acc, (name, value)) =>
        acc.flatMap { c =>
          name match {
            case "host"     => Right(c.copy(host = value))
            case "port"     => value.toIntOption.filter(p => p >= 0 && p <= 65535).map(p => c.copy(port = p)).toRight(s"not a port: '$value'")
            case "data-dir" => Right(c.copy(dataDir = Paths.get(value)))
            case "db" =>
              value.toLowerCase match {
                case "sqlite" => Right(c.copy(database = Database.Sqlite))
                case "memory"           => Right(c.copy(database = Database.Memory))
                case "mongodb" | "mongo" => Right(c.copy(database = Database.Mongo))
                case other              => Left(s"--db must be sqlite, mongodb or memory, not '$other'")
              }
            case "node-timeout-seconds" => value.toLongOption.filter(_ > 0).map(s => c.copy(nodeTimeout = s.seconds)).toRight(s"not a number of seconds: '$value'")
            case "mongo-uri"            => Either.cond(value.startsWith("mongodb://") || value.startsWith("mongodb+srv://"), c.copy(mongo = c.mongo.copy(uri = value)), "--mongo-uri must start with mongodb:// or mongodb+srv://")
            case "mongo-database"       => Either.cond(value.nonEmpty && !value.exists(ch => "/\\. \"$".contains(ch)), c.copy(mongo = c.mongo.copy(database = value)), s"not a MongoDB database name: '$value'")
            case "examples"             => value.toLowerCase.toBooleanOption.map(b => c.copy(examples = b)).toRight(s"--examples must be true or false, not '$value'")
            case "pool-idle-seconds"     => seconds(value).map(d => c.copy(pool = c.pool.copy(idle = d)))
            case "pool-acquire-seconds"  => seconds(value).map(d => c.copy(pool = c.pool.copy(acquireTimeout = d)))
            case "pool-shutdown-seconds" => seconds(value).map(d => c.copy(pool = c.pool.copy(shutdownGrace = d)))
            case "pool-max"              => value.toIntOption.filter(_ > 0).map(n => c.copy(pool = c.pool.copy(max = n))).toRight(s"--pool-max must be a positive number, not '$value'")
            case "plugins-dir"          => Right(c.copy(pluginsDir = Some(Paths.get(value))))
            case "plugins-strict"       => value.toLowerCase.toBooleanOption.map(b => c.copy(pluginsStrict = b)).toRight(s"--plugins-strict must be true or false, not '$value'")
            case "scala-nodes"          => value.toLowerCase.toBooleanOption.map(b => c.copy(scalaNodes = b)).toRight(s"--scala-nodes must be true or false, not '$value'")
            case "otel-endpoint" =>
              Either.cond(value.startsWith("http://") || value.startsWith("https://"), c.copy(otel = Some(c.otel.fold(Otel(value.stripSuffix("/")))(_.copy(endpoint = value.stripSuffix("/"))))), s"--otel-endpoint must be an http(s) URL, not '$value'")
            case "otel-service" => Right(c.copy(otel = c.otel.map(_.copy(service = value)).orElse(Some(Otel("", value)))))
            case "ai-record"            => Right(c.copy(aiCassettes = AiCassettes.Record(Paths.get(value))))
            case "ai-replay"            => Right(c.copy(aiCassettes = AiCassettes.Replay(Paths.get(value))))
            case other                  => Left(s"unknown option --$other")
          }
        }
      }
    }.map(endpointOnly)
  }
}
