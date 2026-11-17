package xplat.flow.stream

import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.Traces

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.{Duration => JDuration}

/** Sends each element to an HTTP endpoint. With POST, PUT or PATCH the element is the JSON body. GET and DELETE have no body: the
  * element goes in the query string instead, one parameter per field of an object (text and numbers as they are, nested values as JSON
  * text, `null` left out), or as `value=<json>` when it is not an object. Any answer that is not a 2xx makes the element a dead letter. It uses
  * the HTTP client of the JDK. It writes to the first output it declares how many were accepted.
  */
final class HttpSink[F[_]](traces: Option[Traces[F]] = None)(implicit F: Async[F]) extends StreamSink[F] {

  val descriptor: NodeDescriptor = HttpSink.descriptor

  private lazy val client: HttpClient = HttpClient.newHttpClient()

  def drain(ctx: StreamContext[F], upstream: Stream[F, Elem[F]]): F[Map[String, Json]] =
    HttpSink.params(ctx.params) match {
      case Left(problem) => F.raiseError(new IllegalArgumentException(problem))
      case Right(p) =>
        // Every request carries the `traceparent` of the segment, unless the headers set one.
        val traceparent =
          if (p.headers.keys.exists(_.equalsIgnoreCase(Traces.Header))) F.pure(Option.empty[String])
          else traces.fold(F.pure(Option.empty[String]))(_.traceparent(ctx.input.instanceId, ctx.input.nodeId))
        (Ref.of[F, Long](0L), traceparent).tupled.flatMap { case (accepted, tp) =>
          upstream.evalMap { e =>
            val base = HttpRequest.newBuilder().timeout(JDuration.ofSeconds(p.timeoutSeconds))
            val builder =
              if (HttpSink.WithoutBody(p.method)) base.uri(URI.create(HttpSink.withQuery(p.url, e.value))).method(p.method, HttpRequest.BodyPublishers.noBody())
              else base.uri(URI.create(p.url)).header("Content-Type", "application/json").method(p.method, HttpRequest.BodyPublishers.ofString(e.value.noSpaces))
            val request = (p.headers ++ tp.map(Traces.Header -> _)).foldLeft(builder)((b, h) => b.header(h._1, h._2)).build()
            F.fromCompletableFuture(F.delay(client.sendAsync(request, HttpResponse.BodyHandlers.discarding()))).attempt.flatMap {
              case Right(r) if r.statusCode() / 100 == 2 => accepted.update(_ + 1) >> e.ack
              case Right(r)                              => ctx.fail(e, s"${p.method} ${p.url} answered ${r.statusCode()}")
              case Left(error)                           => ctx.fail(e, s"${p.method} ${p.url} failed: ${Option(error.getMessage).getOrElse(error.toString)}")
            }
          }.compile.drain >> accepted.get.map(n => ctx.result(Json.fromLong(n)))
        }
    }
}

object HttpSink {
  val Kind: String = "stream.sink.http"

  final case class Params(url: String, method: String, headers: Map[String, String], timeoutSeconds: Long)

  private val Methods = Set("POST", "PUT", "PATCH", "GET", "DELETE")

  /** The methods that send the element in the query string, not in the body. */
  val WithoutBody: Set[String] = Set("GET", "DELETE")

  /** `url` with the element added to its query string (after the parameters it already has). */
  def withQuery(url: String, element: Json): String = {
    def text(v: Json): Option[String] = if (v.isNull) None else Some(v.asString.getOrElse(v.noSpaces))
    val pairs = element.asObject match {
      case Some(o) => o.toList.flatMap { case (k, v) => text(v).map(k -> _) }
      case None    => text(element).map("value" -> _).toList
    }
    def enc(t: String) = java.net.URLEncoder.encode(t, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20")
    if (pairs.isEmpty) url
    else {
      val (path, fragment) = url.span(_ != '#')
      val sep = if (path.contains('?')) (if (path.endsWith("?") || path.endsWith("&")) "" else "&") else "?"
      path + sep + pairs.map { case (k, v) => s"${enc(k)}=${enc(v)}" }.mkString("&") + fragment
    }
  }

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    for {
      url <- c.get[String]("url").left.map(_ => "missing field 'url'")
      uri <- scala.util.Try(URI.create(url)).toEither.left.map(_ => s"'$url' is not a URL")
      _ <- Either.cond(Option(uri.getScheme).exists(s => s == "http" || s == "https"), (), s"'$url' must start with http:// or https://")
      method <- c.getOrElse[String]("method")("POST").left.map(_.message).map(_.toUpperCase)
      _ <- Either.cond(Methods(method), (), s"method must be one of ${Methods.toList.sorted.mkString(", ")}")
      headers <- c.getOrElse[Map[String, String]]("headers")(Map.empty).left.map(_.message)
      timeout <- c.getOrElse[Long]("timeoutSeconds")(30L).left.map(_.message)
      _ <- Either.cond(timeout > 0, (), "timeoutSeconds must be more than 0")
    } yield Params(url, method, headers, timeout)
  }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = StreamPorts.SinkCategory,
    label = "HTTP",
    description = "Ends a stream by sending each element to an HTTP endpoint.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("url", DataType.Str),
        ConfigSchema.Field("method", DataType.Str, required = false, default = Some(Json.fromString("POST")), description = Some("GET and DELETE send the element in the query string"), oneOf = Methods.toList.sorted.map(Json.fromString)),
        ConfigSchema.Field("headers", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("Headers to send, as text")),
        ConfigSchema.Field("timeoutSeconds", DataType.Num, required = false, default = Some(Json.fromInt(30)))
      )
    ),
    sideEffects = SideEffects.Write,
    inputs = _ => StreamPorts.input,
    defaultOutputs = _ => List(OutputDecl("result", StatePath(List("sent")), DataType.Num)),
    checkParams = p => params(p).left.toOption.toList
  )
}
