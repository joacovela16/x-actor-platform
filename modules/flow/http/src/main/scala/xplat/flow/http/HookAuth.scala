package xplat.flow.http

import cats.effect.Sync
import cats.syntax.all._

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** Checks that a request to a trigger carries what [[TriggerDef.Auth]] asks for. */
object HookAuth {

  /** @param header
    *   a header of the request by name, in any case.
    */
  def check[F[_]: Sync](auth: Option[TriggerDef.Auth], header: String => Option[String], body: Array[Byte], secrets: SecretStore[F]): F[Boolean] =
    auth match {
      case None => Sync[F].pure(true)
      case Some(a) =>
        // A secret that is not set means nobody can pass, not that anybody can.
        secrets.get(a.secret).map {
          case None => false
          case Some(secret) =>
            a.scheme match {
              case "apiKey" => header(a.header.getOrElse("X-Api-Key")).exists(same(_, secret))
              case "bearer" => header("Authorization").collect { case h if h.regionMatches(true, 0, "Bearer ", 0, 7) => h.drop(7).trim }.exists(same(_, secret))
              case "hmac"   => header(a.header.getOrElse("X-Signature")).exists(sig => same(sig.trim.stripPrefix("sha256="), hmac(secret, body)))
              case _        => false
            }
        }
    }

  private def hmac(secret: String, body: Array[Byte]): String = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes(UTF_8), "HmacSHA256"))
    mac.doFinal(body).map(b => f"${b & 0xff}%02x").mkString
  }

  /** Compares without stopping at the first difference. */
  private def same(a: String, b: String): Boolean = MessageDigest.isEqual(a.getBytes(UTF_8), b.getBytes(UTF_8))

  /** The signature that a client has to send for `body`, for tests and documentation. */
  def sign(secret: String, body: Array[Byte]): String = "sha256=" + hmac(secret, body)
}
