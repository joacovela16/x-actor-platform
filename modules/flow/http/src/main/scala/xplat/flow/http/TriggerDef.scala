package xplat.flow.http

import io.circe.{Decoder, DecodingFailure, Encoder, Json}

/** What starts a flow, apart from a person: a request to `/hooks/<path>`, a schedule or a queue. It is stored apart from the
  * flows, and the JSON is flat.
  *
  * @param inputs
  *   how the inputs of the flow are filled from the request of an HTTP trigger, by name of the input, with the references of
  *   [[xplat.flow.model.Refs]]: `${request.body}`, `${request.body.order.id}`, `${request.query.id}`, `${request.headers.x-user}`,
  *   `${request.method}`, `${request.path}`, and `${env.NAME}`; a whole reference keeps the type of the value. The older `$query.id`
  *   still works. Any other value is used as it is. Without `inputs`, the body of the request is the inputs when it is an object.
  * @param timeoutMs
  *   how long a synchronous trigger waits for the response before it answers 202 (10 seconds if it is empty).
  * @param connection
  *   of a `queue` trigger: the named connection to the broker (see the connection kinds `kafka`, `activemq`, `redis`). `queue` is
  *   then the topic, queue or stream to read. With a `mongodb` connection `queue` is a collection, and each change to it (insert,
  *   update, replace, delete) is a message whose body is the change (`${message.body.operationType}`, `${message.body.fullDocument}`...). The `inputs` references are `${message.body}`, `${message.key}`, `${message.headers.x}`, `${message.source}` and `${message.position}`.
  * @param group
  *   of a `queue` trigger: the consumer group (`flow-<id>` if empty). Servers that share a group share the messages.
  * @param destinationType
  *   of a `queue` trigger on ActiveMQ: `queue` (default) or `topic`.
  * @param timezone
  *   of a `cron` trigger: the IANA time zone the expression is read in (`UTC` if empty). Its `inputs` are used as they are (there is
  *   no request to read from) as the inputs of the instances that it starts.
  */
final case class TriggerDef(
    id: String,
    flowId: String,
    kind: TriggerDef.Kind,
    method: Option[String] = None,
    path: Option[String] = None,
    cron: Option[String] = None,
    queue: Option[String] = None,
    inputs: Map[String, Json] = Map.empty,
    auth: Option[TriggerDef.Auth] = None,
    response: TriggerDef.ResponseMode = TriggerDef.ResponseMode.Async,
    timeoutMs: Option[Long] = None,
    connection: Option[String] = None,
    group: Option[String] = None,
    destinationType: Option[String] = None,
    timezone: Option[String] = None
) {

  /** The path of an HTTP trigger without the slashes at its ends. */
  def route: String = path.getOrElse("").split("/").filter(_.nonEmpty).mkString("/")

  /** The consumer group of a `queue` trigger: its `group`, or one of its own. */
  def consumerGroup: String = group.filter(_.nonEmpty).getOrElse(s"flow-$id")

  /** The method of an HTTP trigger, `POST` if it does not say. */
  def httpMethod: String = method.fold("POST")(_.toUpperCase)

  /** Where the references of its inputs read from: `request` for an HTTP trigger, `message` for a queue one. */
  def origin: String = if (kind == TriggerDef.Kind.Queue) "message" else "request"

  /** The origins its inputs may use: its own, and `env`. A cron or manual trigger has no request: its inputs are used as they are. */
  def origins: Set[String] = kind match {
    case TriggerDef.Kind.Http | TriggerDef.Kind.Queue => Set(origin, "env")
    case _                                            => Set.empty
  }

  /** What is wrong with it, apart from the flow not existing. */
  def problems: List[String] = {
    val common = (if (id.matches("[A-Za-z0-9_.-]+")) Nil else List("the id may only have letters, digits, '_', '.' and '-'")) ++
      (if (flowId.trim.nonEmpty) Nil else List("flowId is empty"))
    val own = kind match {
      case TriggerDef.Kind.Http =>
        (if (route.nonEmpty) Nil else List("an http trigger needs a path")) ++
          (if (TriggerDef.Methods(httpMethod)) Nil else List(s"unknown method '$httpMethod'")) ++
          auth.toList.flatMap(_.problems)
      case TriggerDef.Kind.Cron =>
        (cron.filter(_.trim.nonEmpty) match {
          case None             => List("a cron trigger needs a cron expression")
          case Some(expression) => xplat.flow.model.CronExpression.parse(expression).left.toOption.map(e => s"cron: $e").toList
        }) ++ timezone.toList.flatMap(z => xplat.flow.engine.CronSchedule.zone(z).left.toOption.toList)
      case TriggerDef.Kind.Queue =>
        (if (queue.exists(_.trim.nonEmpty)) Nil else List("a queue trigger needs a queue")) ++
          (if (connection.exists(_.trim.nonEmpty)) Nil else List("a queue trigger needs a connection")) ++
          (if (destinationType.forall(TriggerDef.DestinationTypes)) Nil else List(s"destinationType must be one of ${TriggerDef.DestinationTypes.toList.sorted.mkString(", ")}"))
      case TriggerDef.Kind.Manual => Nil
    }
    val refs = inputs.toList.sortBy(_._1).flatMap { case (name, v) => xplat.flow.model.Refs.problems(v, origins, "a " + kind.name + " trigger").map(p => s"input '$name': $p") }
    common ++ own ++ refs
  }
}

object TriggerDef {

  val Methods: Set[String] = Set("GET", "POST", "PUT", "PATCH", "DELETE")

  val DestinationTypes: Set[String] = Set("queue", "topic")

  sealed trait Kind { def name: String }
  object Kind {
    case object Http extends Kind { val name = "http" }
    case object Cron extends Kind { val name = "cron" }
    case object Queue extends Kind { val name = "queue" }
    case object Manual extends Kind { val name = "manual" }

    val all: List[Kind] = List(Http, Cron, Queue, Manual)

    implicit val encoder: Encoder[Kind] = Encoder.encodeString.contramap(_.name)
    implicit val decoder: Decoder[Kind] =
      Decoder.decodeString.emap(s => all.find(_.name == s).toRight(s"unknown trigger kind '$s'"))
  }

  /** Async answers 202 with the id of the instance. Sync waits for a `core.respond` node and answers what it describes. */
  sealed trait ResponseMode { def name: String }
  object ResponseMode {
    case object Async extends ResponseMode { val name = "async" }
    case object Sync extends ResponseMode { val name = "sync" }

    implicit val encoder: Encoder[ResponseMode] = Encoder.encodeString.contramap(_.name)
    implicit val decoder: Decoder[ResponseMode] =
      Decoder.decodeString.emap {
        case "async" => Right(Async)
        case "sync"  => Right(Sync)
        case other   => Left(s"unknown response mode '$other'")
      }
  }

  /** How a request proves who it is, with a secret that is kept in the [[SecretStore]] under the name `secret`, which is
    * `<connection>.<field>`.
    *
    * `apiKey` compares the header (`X-Api-Key` by default) with the secret; `bearer` compares the token of `Authorization:
    * Bearer`; `hmac` checks that the header (`X-Signature` by default, as `sha256=<hex>`) is the HMAC-SHA256 of the body.
    */
  final case class Auth(scheme: String, secret: String, header: Option[String] = None) {
    def problems: List[String] =
      (if (Auth.Schemes(scheme)) Nil else List(s"unknown auth scheme '$scheme'")) ++
        (if (secret.trim.nonEmpty) Nil else List("auth needs a secret"))
  }
  object Auth {
    val Schemes: Set[String] = Set("apiKey", "bearer", "hmac")
    implicit val encoder: Encoder[Auth] = Encoder.forProduct3("scheme", "secret", "header")((a: Auth) => (a.scheme, a.secret, a.header)).mapJsonObject(_.filter(!_._2.isNull))
    implicit val decoder: Decoder[Auth] = Decoder.forProduct3("scheme", "secret", "header")(Auth.apply)
  }

  implicit val encoder: Encoder[TriggerDef] = Encoder.instance { t =>
    Json
      .obj(
        "id" -> Json.fromString(t.id),
        "flowId" -> Json.fromString(t.flowId),
        "kind" -> Encoder[Kind].apply(t.kind),
        "method" -> Encoder[Option[String]].apply(t.method),
        "path" -> Encoder[Option[String]].apply(t.path),
        "cron" -> Encoder[Option[String]].apply(t.cron),
        "queue" -> Encoder[Option[String]].apply(t.queue),
        "inputs" -> (if (t.inputs.isEmpty) Json.Null else Json.fromFields(t.inputs)),
        "auth" -> Encoder[Option[Auth]].apply(t.auth),
        "response" -> (if (t.response == ResponseMode.Async) Json.Null else Encoder[ResponseMode].apply(t.response)),
        "timeoutMs" -> Encoder[Option[Long]].apply(t.timeoutMs),
        "connection" -> Encoder[Option[String]].apply(t.connection),
        "group" -> Encoder[Option[String]].apply(t.group),
        "destinationType" -> Encoder[Option[String]].apply(t.destinationType),
        "timezone" -> Encoder[Option[String]].apply(t.timezone)
      )
      .mapObject(_.filter { case (_, v) => !v.isNull })
  }

  implicit val decoder: Decoder[TriggerDef] = Decoder.instance { c =>
    for {
      id <- c.get[String]("id")
      flowId <- c.get[String]("flowId")
      kind <- c.get[Kind]("kind")
      method <- c.get[Option[String]]("method")
      path <- c.get[Option[String]]("path")
      cron <- c.get[Option[String]]("cron")
      queue <- c.get[Option[String]]("queue")
      inputs <- c.getOrElse[Map[String, Json]]("inputs")(Map.empty)
      auth <- c.get[Option[Auth]]("auth")
      response <- c.getOrElse[ResponseMode]("response")(ResponseMode.Async)
      timeoutMs <- c.get[Option[Long]]("timeoutMs")
      connection <- c.get[Option[String]]("connection")
      group <- c.get[Option[String]]("group")
      destinationType <- c.get[Option[String]]("destinationType")
      timezone <- c.get[Option[String]]("timezone")
      _ <- if (timeoutMs.forall(_ > 0)) Right(()) else Left(DecodingFailure("timeoutMs must be positive", c.history))
    } yield TriggerDef(id, flowId, kind, method, path, cron, queue, inputs, auth, response, timeoutMs, connection, group, destinationType, timezone)
  }
}
