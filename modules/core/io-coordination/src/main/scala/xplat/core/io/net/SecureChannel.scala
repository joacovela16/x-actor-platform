package xplat.core.io.net

import cats.effect.kernel.{Async, Resource}
import cats.effect.syntax.all._
import cats.implicits._
import fs2.io.net.tls.{TLSContext, TLSParameters}
import fs2.io.net.{Network, Socket}
import xplat.core.api.net.{TlsConfig, TransportSecurity}

import java.security.{KeyStore, MessageDigest, SecureRandom}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import scala.concurrent.duration._

/** Opens connections protected by [[TransportSecurity]]: mutual TLS around the socket, then a handshake in which both
  * sides prove they know the shared secret.
  *
  * The handshake is three frames: the server sends a random challenge, the client answers with its own payload and an
  * HMAC of the challenge and payload, and the server answers with its payload and an HMAC of a client nonce and that
  * payload. The secret never travels, a proof cannot be replayed on another connection, and neither side can be
  * impersonated or have its payload altered without the secret. Without a secret the proofs are empty and only the
  * payloads are exchanged.
  */
final class SecureChannel[F[_]] private (tls: Option[TLSContext[F]], secret: Option[String])(implicit A: Async[F]) {
  import SecureChannel._

  /** Wraps an accepted socket in TLS when configured, requiring the client's certificate. */
  def server(socket: Socket[F]): Resource[F, Socket[F]] =
    tls.fold(Resource.pure[F, Socket[F]](socket))(_.serverBuilder(socket).withParameters(TLSParameters(needClientAuth = true)).build.widen[Socket[F]])

  /** Wraps an outgoing socket in TLS when configured. */
  def client(socket: Socket[F]): Resource[F, Socket[F]] =
    tls.fold(Resource.pure[F, Socket[F]](socket))(_.clientBuilder(socket).build.widen[Socket[F]])

  /** Server side of the handshake. `respond` sees the client's payload and returns the server's. Returns the client's
    * payload, or `None` if the client does not know the secret.
    */
  def accept(socket: Socket[F])(respond: Array[Byte] => F[Array[Byte]]): F[Option[Array[Byte]]] = {
    val challenge = nonce()
    write(socket, Challenge(challenge)) >> readFrame(socket).timeout(HandshakeTimeout).flatMap {
      case Hello(payload, clientNonce, proof) if verify("client", challenge, payload, proof) =>
        respond(payload).flatMap(answer => write(socket, Welcome(answer, this.proof("server", clientNonce, answer)))).as(Option(payload))
      case Hello(_, _, _) => A.pure(None)
      case other          => A.raiseError(new java.io.IOException(s"unexpected $other during the handshake"))
    }
  }

  /** Client side of the handshake. Returns the server's payload; fails if the server does not know the secret. */
  def connect(socket: Socket[F], payload: Array[Byte]): F[Array[Byte]] =
    for {
      challenge <- readFrame(socket).timeout(HandshakeTimeout).flatMap {
        case Challenge(n) => A.pure(n)
        case other        => A.raiseError[Array[Byte]](new java.io.IOException(s"unexpected $other during the handshake"))
      }
      own = nonce()
      _ <- write(socket, Hello(payload, own, proof("client", challenge, payload)))
      answer <- readFrame(socket).timeout(HandshakeTimeout).flatMap {
        case Welcome(answer, proof) if verify("server", own, answer, proof) => A.pure(answer)
        case Welcome(_, _) => A.raiseError[Array[Byte]](new SecurityException("the server does not know the shared secret"))
        case other         => A.raiseError[Array[Byte]](new java.io.IOException(s"unexpected $other during the handshake"))
      }
    } yield answer

  /** HMAC of role, challenge and payload; the role keeps a proof from being echoed back by the other side. */
  private def proof(role: String, challenge: Array[Byte], payload: Array[Byte]): Array[Byte] =
    secret.fold(Array.emptyByteArray) { s =>
      val mac = Mac.getInstance("HmacSHA256")
      mac.init(new SecretKeySpec(s.getBytes("UTF-8"), "HmacSHA256"))
      mac.update(role.getBytes("UTF-8"))
      mac.update(challenge)
      mac.doFinal(payload)
    }

  private def verify(role: String, challenge: Array[Byte], payload: Array[Byte], given: Array[Byte]): Boolean =
    secret.isEmpty || MessageDigest.isEqual(proof(role, challenge, payload), given)
}

object SecureChannel {

  val HandshakeTimeout: FiniteDuration = 10.seconds

  /** Loads the key and trust stores once; the result is shared by every connection. */
  def apply[F[_]](network: Network[F], security: TransportSecurity)(implicit A: Async[F]): F[SecureChannel[F]] =
    security.tls.traverse(tlsContext(network, _)).map(new SecureChannel[F](_, security.sharedSecret))

  /** Whether two processes with these settings can talk: the same shared secret, and with TLS on both sides, each
    * certificate trusted by the other's trust store. Lets in-process transports reject what a real connection would.
    */
  def compatible(a: TransportSecurity, b: TransportSecurity): Boolean =
    a.sharedSecret == b.sharedSecret && ((a.tls, b.tls) match {
      case (None, None)         => true
      case (Some(x), Some(y))   => trusts(x, y) && trusts(y, x)
      case _                    => false
    })

  /** Whether `server` trusts the certificate that `client` presents. */
  private def trusts(server: TlsConfig, client: TlsConfig): Boolean = {
    val keys = loadStore(client.keyStore, client.keyStorePassword)
    val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trust.init(loadStore(server.trustStore, server.trustStorePassword))
    val manager = trust.getTrustManagers.collectFirst { case m: javax.net.ssl.X509TrustManager => m }
    val chains = scala.jdk.CollectionConverters.EnumerationHasAsScala(keys.aliases()).asScala.filter(keys.isKeyEntry).map(keys.getCertificateChain)
    manager.exists { m =>
      chains.exists { chain =>
        val certs = chain.collect { case c: java.security.cert.X509Certificate => c }
        scala.util.Try(m.checkClientTrusted(certs, certs.head.getPublicKey.getAlgorithm)).isSuccess
      }
    }
  }

  private def loadStore(path: java.nio.file.Path, password: String): KeyStore = {
    val store = KeyStore.getInstance(if (path.toString.endsWith(".jks")) "JKS" else "PKCS12")
    val in = java.nio.file.Files.newInputStream(path)
    try store.load(in, password.toCharArray)
    finally in.close()
    store
  }

  private def tlsContext[F[_]](network: Network[F], config: TlsConfig)(implicit A: Async[F]): F[TLSContext[F]] = A.blocking {
    def load(path: java.nio.file.Path, password: String): KeyStore = loadStore(path, password)
    val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keys.init(load(config.keyStore, config.keyStorePassword), config.keyPassword.getOrElse(config.keyStorePassword).toCharArray)
    val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trust.init(load(config.trustStore, config.trustStorePassword))
    val context = SSLContext.getInstance("TLS")
    context.init(keys.getKeyManagers, trust.getTrustManagers, new SecureRandom())
    network.tlsContext.fromSSLContext(context)
  }

  private def nonce(): Array[Byte] = { val n = new Array[Byte](32); new SecureRandom().nextBytes(n); n }

  // ---- Handshake frames. Tags 1 to 3 are reserved for them; protocols on top use other tags. ----

  private sealed trait Frame
  private final case class Challenge(nonce: Array[Byte]) extends Frame
  private final case class Hello(payload: Array[Byte], nonce: Array[Byte], proof: Array[Byte]) extends Frame
  private final case class Welcome(payload: Array[Byte], proof: Array[Byte]) extends Frame

  private def write[F[_]](socket: Socket[F], frame: Frame): F[Unit] = {
    val w = BinWriter()
    frame match {
      case Challenge(n)       => w.byte(1).bytes(n)
      case Hello(p, n, proof) => w.byte(2).bytes(p).bytes(n).bytes(proof)
      case Welcome(p, proof)  => w.byte(3).bytes(p).bytes(proof)
    }
    Frames.write(socket, w.result())
  }

  private def readFrame[F[_]](socket: Socket[F])(implicit A: Async[F]): F[Frame] =
    Frames.read(socket).take(1).compile.last.flatMap {
      case None => A.raiseError(new java.io.IOException("connection closed during the handshake"))
      case Some(bytes) =>
        A.delay {
          val r = BinReader(bytes)
          r.byte() match {
            case 1     => Challenge(r.bytes())
            case 2     => Hello(r.bytes(), r.bytes(), r.bytes())
            case 3     => Welcome(r.bytes(), r.bytes())
            case other => throw new java.io.IOException(s"unexpected frame $other during the handshake")
          }
        }
    }
}
