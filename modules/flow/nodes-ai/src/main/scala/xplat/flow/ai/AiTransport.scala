package xplat.flow.ai

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.Json
import io.circe.parser.parse
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.typelevel.ci.CIString

/** What the AI node needs from the network: send a JSON body and get the status and the JSON answer. The one over an http4s `Client`
  * is the real one; tests and recordings (see [[Cassettes]]) provide others.
  */
trait AiTransport[F[_]] {
  def post(url: String, headers: List[(String, String)], body: Json): F[AiTransport.Reply]
}

object AiTransport {

  /** @param retryAfterSeconds
    *   what the `Retry-After` header asked for, if it was there.
    */
  final case class Reply(status: Int, body: Json, retryAfterSeconds: Option[Long])

  def http[F[_]](client: Client[F])(implicit F: Concurrent[F]): AiTransport[F] = new AiTransport[F] {
    def post(url: String, headers: List[(String, String)], body: Json): F[Reply] =
      for {
        uri <- F.fromEither(Uri.fromString(url).leftMap(e => new IllegalArgumentException(s"not a valid url: ${e.message}")))
        request = Request[F](Method.POST, uri, headers = Headers(headers.map { case (k, v) => Header.Raw(CIString(k), v) })).withEntity(body)
        reply <- client.run(request).use { response =>
          response.bodyText.compile.string.map { text =>
            Reply(response.status.code, parse(text).getOrElse(Json.fromString(text)), response.headers.get(CIString("retry-after")).flatMap(_.head.value.trim.toLongOption))
          }
        }
      } yield reply
  }
}
