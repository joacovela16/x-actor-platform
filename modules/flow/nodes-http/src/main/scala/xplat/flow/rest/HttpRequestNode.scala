package xplat.flow.rest

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.{Connections, NodeType, Template, Traces}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration => JDuration}
import scala.jdk.CollectionConverters._

/** Calls an HTTP service and answers what it said, in the outputs it declares:
  *
  *  - `body`: the body of the response, parsed when it is JSON, as text otherwise (`null` if empty);
  *  - `status`: the status code;
  *  - `headers`: the headers of the response, by name in lower case (the first value of each).
  *
  * `url` can be absolute or, with a `connection` of kind `http`, relative to its `baseUrl` (whose `token` is sent as `Authorization:
  * Bearer`). `${state.path}` in the URL is replaced by that value of the state, encoded (the older `{{path}}` still works). `query`,
  * `headers` and `body` take values of the run with the references of [[Template]] (`"${state.order.id}"`). A status that is not 2xx goes out by `error` unless `failOnStatus` is
  * false. In test mode it does not call: the audit shows the request it would have made.
  */
final class HttpRequestNode[F[_]](connections: Connections[F], traces: Option[Traces[F]] = None)(implicit F: Async[F]) extends NodeType[F] {

  val descriptor: NodeDescriptor = HttpRequestNode.descriptor

  private lazy val client: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(JDuration.ofSeconds(10)).build()

  def run(input: NodeInput): F[NodeOutput] =
    HttpRequestNode.params(input.params) match {
      case Left(problem) => NodeOutput.failed(problem).pure[F]
      case Right(p) =>
        p.connection.traverse(connections.resolve).flatMap {
          case Some(None) => NodeOutput.failed(s"the connection '${p.connection.getOrElse("")}' does not exist").pure[F]
          case found =>
            val conn = found.flatten
            HttpRequestNode.request(p, input, conn) match {
              case Left(problem) => NodeOutput.failed(problem).pure[F]
              case Right(plain) =>
                traced(plain, input).flatMap { r =>
                val secrets = conn.toList.flatMap(_.secrets.values).filter(_.nonEmpty)
                def clean(t: String) = secrets.foldLeft(t)((acc, s) => acc.replace(s, "***"))
                val started = System.nanoTime()
                F.fromCompletableFuture(F.delay(client.sendAsync(HttpRequestNode.build(r, p.timeoutSeconds), HttpResponse.BodyHandlers.ofByteArray())))
                  .map(resp => HttpRequestNode.answer(p, r, input, resp, (System.nanoTime() - started) / 1000000))
                  .handleError(e => NodeOutput.failed(clean(s"${r.method} ${r.url} failed: ${Option(e.getMessage).getOrElse(e.toString)}")))
                }
            }
        }
    }

  /** The request with the `traceparent` of this node, unless it sets one itself. */
  private def traced(r: HttpRequestNode.Request, input: NodeInput): F[HttpRequestNode.Request] =
    if (r.headers.exists(_._1.equalsIgnoreCase(Traces.Header))) F.pure(r)
    else traces.fold(F.pure(Option.empty[String]))(_.traceparent(input.instanceId, input.nodeId)).map(_.fold(r)(t => r.copy(headers = r.headers :+ (Traces.Header -> t))))

  override def simulated(input: NodeInput): Option[NodeOutput] =
    Some(HttpRequestNode.params(input.params).flatMap(p => HttpRequestNode.request(p, input, None).map(p -> _)) match {
      case Left(problem) => NodeOutput.failed(problem)
      case Right((p, r)) =>
        val all = Map("body" -> Json.Null, "status" -> Json.fromInt(200), "headers" -> Json.obj())
        val wanted = input.outputs.map(_.name)
        NodeOutput(if (wanted.isEmpty) all else all.filter { case (k, _) => wanted.contains(k) }, info = HttpRequestNode.info(r) + ("simulated" -> Json.True))
    })
}

object HttpRequestNode {

  val Kind: String = "http.request"

  val Methods: List[String] = List("GET", "POST", "PUT", "PATCH", "DELETE")

  final case class Params(
      connection: Option[String],
      method: String,
      url: String,
      query: Json,
      headers: Json,
      body: Option[Json],
      failOnStatus: Boolean,
      timeoutSeconds: Int
  )

  /** The request with every value of the run in place. */
  final case class Request(method: String, url: String, headers: List[(String, String)], body: Option[Json])

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      connection <- c.get[Option[String]]("connection").left.map(_.message).map(_.filter(_.trim.nonEmpty))
      method <- c.getOrElse[String]("method")("GET").left.map(_.message).map(_.toUpperCase)
      _ <- Either.cond(Methods.contains(method), (), s"method must be one of ${Methods.mkString(", ")}")
      url <- c.get[String]("url").left.map(_ => "missing field 'url'").filterOrElse(_.trim.nonEmpty, "url is empty")
      _ <- Either.cond(connection.isDefined || url.startsWith("http://") || url.startsWith("https://"), (), "url must start with http:// or https://, or be relative to a connection")
      query <- c.getOrElse[Json]("query")(Json.obj()).left.map(_.message).filterOrElse(j => j.isObject || j.isNull, "query must be an object")
      headers <- c.getOrElse[Json]("headers")(Json.obj()).left.map(_.message).filterOrElse(j => j.isObject || j.isNull, "headers must be an object")
      body <- c.get[Option[Json]]("body").left.map(_.message).map(_.filterNot(_.isNull))
      _ <- Either.cond(body.isEmpty || method != "GET", (), "a GET has no body: use query")
      _ <- Template.textProblems(url).headOption.toLeft(())
      _ <- (Template.problems(query) ++ Template.problems(headers) ++ body.toList.flatMap(Template.problems)).headOption.toLeft(())
      failOnStatus <- c.getOrElse[Boolean]("failOnStatus")(true).left.map(_.message)
      timeout <- c.getOrElse[Int]("timeoutSeconds")(30).left.map(_.message).filterOrElse(t => t > 0 && t <= 600, "timeoutSeconds must be between 1 and 600")
    } yield Params(connection, method, url, query, headers, body, failOnStatus, timeout)
  }

  private val Placeholder = "\\{\\{\\s*([^}\\s]+)\\s*\\}\\}".r

  private def enc(t: String): String = java.net.URLEncoder.encode(t, UTF_8).replace("+", "%20")

  private def text(v: Json): String = v.asString.getOrElse(v.noSpaces)

  /** The paths of the state that `{{...}}` in the URL reads. */
  def urlPaths(url: String): List[StatePath] = (Template.textReads(url) ++ Placeholder.findAllMatchIn(url).toList.flatMap(m => StatePath.parse(m.group(1)).toOption)).distinct

  def request(p: Params, input: NodeInput, connection: Option[Connections.Resolved]): Either[String, Request] =
    for {
      legacy <- Placeholder.findAllMatchIn(p.url).toList.foldLeft[Either[String, String]](Right(p.url)) { (acc, m) =>
        acc.flatMap { u =>
          StatePath.parse(m.group(1)).left.map(_ => s"'{{${m.group(1)}}}' in the url is not a path").flatMap { path =>
            path.get(input.state).filterNot(_.isNull).toRight(s"'$path' (in the url) is not in the state").map(v => u.replace(m.matched, enc(text(v))))
          }
        }
      }
      url <- Template.text(legacy, input, enc)
      base = connection.flatMap(_.text("baseUrl")).map(_.stripSuffix("/"))
      absolute <-
        if (url.startsWith("http://") || url.startsWith("https://")) Right(url)
        else base.map(b => s"$b/${url.stripPrefix("/")}").toRight(s"the connection has no baseUrl for '$url'")
      query <- Template.resolve(p.query, input)
      headers <- Template.resolve(p.headers, input)
      body <- p.body.traverse(Template.resolve(_, input))
    } yield {
      val pairs = query.asObject.toList.flatMap(_.toList).collect { case (k, v) if !v.isNull => s"${enc(k)}=${enc(text(v))}" }
      val withQuery = if (pairs.isEmpty) absolute else absolute + (if (absolute.contains('?')) "&" else "?") + pairs.mkString("&")
      val token = connection.flatMap(_.secrets.get("token")).filter(_.nonEmpty).map(t => "Authorization" -> s"Bearer $t")
      val own = headers.asObject.toList.flatMap(_.toList).collect { case (k, v) if !v.isNull => k -> text(v) }
      Request(p.method, withQuery, token.toList ++ own, body)
    }

  private def build(r: Request, timeoutSeconds: Int): HttpRequest = {
    val publisher = r.body.fold(HttpRequest.BodyPublishers.noBody())(b => HttpRequest.BodyPublishers.ofString(b.asString.getOrElse(b.noSpaces)))
    val base = HttpRequest.newBuilder(URI.create(r.url)).timeout(JDuration.ofSeconds(timeoutSeconds.toLong)).header("Accept", "application/json, */*")
    val typed = if (r.body.exists(!_.isString) && !r.headers.exists(_._1.equalsIgnoreCase("Content-Type"))) base.header("Content-Type", "application/json") else base
    r.headers.foldLeft(typed)((b, h) => b.header(h._1, h._2)).method(r.method, publisher).build()
  }

  private def answer(p: Params, r: Request, input: NodeInput, resp: HttpResponse[Array[Byte]], millis: Long): NodeOutput = {
    val raw = new String(resp.body(), UTF_8)
    val body = if (raw.trim.isEmpty) Json.Null else io.circe.parser.parse(raw).getOrElse(Json.fromString(raw))
    val headers = Json.fromFields(resp.headers().map().asScala.toList.sortBy(_._1).flatMap { case (k, vs) => vs.asScala.headOption.map(v => k.toLowerCase -> Json.fromString(v)) })
    val all = Map("body" -> body, "status" -> Json.fromInt(resp.statusCode()), "headers" -> headers)
    val audit = info(r) ++ Map("status" -> Json.fromInt(resp.statusCode()), "durationMs" -> Json.fromLong(millis))
    val wanted = input.outputs.map(_.name)
    val unknown = wanted.filterNot(all.contains)
    if (unknown.nonEmpty) NodeOutput.failed(s"http.request cannot answer ${unknown.map(n => s"'$n'").mkString(", ")}; it answers body, status, headers")
    else if (p.failOnStatus && resp.statusCode() / 100 != 2)
      NodeOutput.failed(s"${r.method} ${r.url} answered ${resp.statusCode()}: ${raw.take(300)}").copy(info = audit)
    else NodeOutput(if (wanted.isEmpty) all else all.filter { case (k, _) => wanted.contains(k) }, info = audit)
  }

  /** What the audit shows of a request: never the value of the Authorization header. */
  private def info(r: Request): Map[String, Json] =
    Map(
      "request" -> Json.obj(
        "method" -> Json.fromString(r.method),
        "url" -> Json.fromString(r.url),
        "headers" -> Json.fromFields(r.headers.map { case (k, v) => k -> Json.fromString(if (k.equalsIgnoreCase("Authorization")) "***" else v) }),
        "body" -> r.body.getOrElse(Json.Null)
      )
    )

  private val jsonUi = UiSpec.code("json", 4)

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "HTTP",
    label = "HTTP request",
    description = "Calls an HTTP service and writes the response in the state (body, status, headers).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("method", DataType.Str, required = false, default = Some(Json.fromString("GET")), oneOf = Methods.map(Json.fromString)),
        ConfigSchema.Field("url", DataType.Str, description = Some("Absolute, or relative to the connection. ${state.path} puts in a value of the state: https://api.test/users/${state.user.id}")),
        ConfigSchema.Field("connection", DataType.Str, required = false, description = Some("Optional connection of kind http: its baseUrl and its token (Bearer)")),
        ConfigSchema.Field("query", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Query parameters: {\"userId\": \"${state.user.id}\", \"limit\": 5}"), ui = Some(jsonUi)),
        ConfigSchema.Field("headers", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Headers to send, by name"), ui = Some(jsonUi)),
        ConfigSchema.Field("body", DataType.AnyJson, required = false, description = Some("Body (JSON) for POST, PUT, PATCH; \"${state.order}\" sends that value of the state"), ui = Some(UiSpec.code("json", 6))),
        ConfigSchema.Field("failOnStatus", DataType.Bool, required = false, default = Some(Json.True), description = Some("A status that is not 2xx goes out by error")),
        ConfigSchema.Field("timeoutSeconds", DataType.Num, required = false, default = Some(Json.fromInt(30)))
      )
    ),
    sideEffects = SideEffects.Write,
    defaultOutputs = _ => List(OutputDecl("body", StatePath(List("response")), DataType.AnyJson)),
    reads = json =>
      params(json).toOption.toList.flatMap(p => (urlPaths(p.url) ++ Template.reads(p.query) ++ Template.reads(p.headers) ++ p.body.toList.flatMap(Template.reads)).distinct).map(StateRead(_, DataType.AnyJson)),
    checkParams = json => params(json).left.toOption.toList
  )
}
