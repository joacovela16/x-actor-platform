package xplat.flow.ai

import cats.effect.Sync
import cats.syntax.all._
import io.circe.Json
import io.circe.parser.parse

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.security.MessageDigest

/** Records the answers of the AI service and plays them back, so a flow that uses an agent can be tested without calling it again
  * (it is slow, it costs, and its answers change).
  *
  * A recording is a file in a directory, named after a hash of the request (url and body; never the headers, so the key is not
  * involved). `Record` goes to the real service and keeps every answer that was a success; `Replay` never goes to the network and
  * fails for a request that has no recording.
  */
object Cassettes {

  sealed trait Mode
  object Mode {
    case object Off extends Mode
    final case class Record(directory: Path) extends Mode
    final case class Replay(directory: Path) extends Mode
  }

  def apply[F[_]](inner: AiTransport[F], mode: Mode)(implicit F: Sync[F]): AiTransport[F] =
    mode match {
      case Mode.Off => inner
      case Mode.Record(directory) =>
        new AiTransport[F] {
          def post(url: String, headers: List[(String, String)], body: Json): F[AiTransport.Reply] =
            inner.post(url, headers, body).flatMap { reply =>
              F.blocking {
                if (reply.status / 100 == 2) {
                  Files.createDirectories(directory)
                  val content = Json.obj("status" -> Json.fromInt(reply.status), "body" -> reply.body)
                  Files.write(file(directory, url, body), content.spaces2.getBytes(UTF_8))
                }
              }.as(reply)
            }
        }
      case Mode.Replay(directory) =>
        new AiTransport[F] {
          def post(url: String, headers: List[(String, String)], body: Json): F[AiTransport.Reply] =
            F.blocking {
              val f = file(directory, url, body)
              if (Files.exists(f)) parse(new String(Files.readAllBytes(f), UTF_8)).toOption else None
            }.flatMap {
              case Some(json) =>
                F.pure(AiTransport.Reply(json.hcursor.get[Int]("status").getOrElse(200), json.hcursor.downField("body").focus.getOrElse(Json.Null), None))
              case None => F.raiseError(new IllegalStateException(s"no recorded answer for this request (${file(directory, url, body).getFileName})"))
            }
        }
    }

  private def file(directory: Path, url: String, body: Json): Path = {
    val digest = MessageDigest.getInstance("SHA-256").digest(s"$url\n${body.noSpaces}".getBytes(UTF_8))
    directory.resolve(digest.take(12).map("%02x".format(_)).mkString + ".json")
  }
}
