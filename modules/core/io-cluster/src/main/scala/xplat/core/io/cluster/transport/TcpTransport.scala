package xplat.core.io.cluster.transport

import cats.effect.kernel.{Async, Clock, Ref, Resource}
import cats.effect.std.Supervisor
import cats.effect.syntax.all._
import cats.implicits._
import com.comcast.ip4s.{Host, Port, SocketAddress}
import fs2.concurrent.SignallingRef
import fs2.io.net.tls.{TLSContext, TLSParameters}
import fs2.io.net.{Network, Socket}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.cluster.api.{Address, TlsConfig, TransportSecurity}
import xplat.core.io.net.{BinReader, BinWriter, Frames}

import java.security.{KeyStore, MessageDigest, SecureRandom}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import scala.concurrent.duration._

/** Transport over TCP, optionally with mutual TLS and a shared secret.
  *
  * Each node keeps one outgoing connection per destination. Every connection starts with a handshake in which both
  * sides prove they know the shared secret (if configured) by answering a random challenge with an HMAC, so the secret
  * never travels.
  *
  * Delivery is reliable while both processes live: messages carry a sequence number, the receiver acknowledges them,
  * and the sender keeps the unacknowledged ones and sends them again after a reconnection, from the last one the
  * receiver reports. The receiver drops duplicates. A process restart is detected by its incarnation number, which
  * resets the sequence. Messages for a node that stays unreachable for [[TcpTransport.GiveUpAfter]] are discarded.
  */
final class TcpTransport[F[_]](outboxSize: Int, ackInterval: FiniteDuration)(implicit A: Async[F]) extends TransportFactory[F] {
  import TcpTransport._

  def bind(system: String, host: String, port: Int, security: TransportSecurity, onReceive: Array[Byte] => F[Unit]): Resource[F, Transport[F]] = {
    val network = Network.forAsync[F]
    for {
      log        <- Resource.eval(Slf4jLogger.fromName[F]("xplat.cluster.tcp"))
      bindHost   <- Resource.eval(A.fromOption(Host.fromString(host), new IllegalArgumentException(s"invalid host $host")))
      bindPort   <- Resource.eval(A.fromOption(Port.fromInt(port), new IllegalArgumentException(s"invalid port $port")))
      tls        <- security.tls.traverse(c => Resource.eval(tlsContext(network, c)))
      supervisor <- Supervisor[F](await = false)
      server     <- network.serverResource(Some(bindHost), Some(bindPort))
      (bound, clients) = server
      self = Address(system, host, bound.port.value)
      incarnation <- Resource.eval(A.delay(new SecureRandom().nextLong()))
      received    <- Resource.eval(Ref.of[F, Map[Address, (Long, Long)]](Map.empty))
      handshake = Handshake(self, incarnation, security.sharedSecret)
      inbound = new Inbound[F](handshake, received, onReceive, ackInterval, log)
      // Each connection is served inside the stream: a socket is closed when its stream element goes out of scope.
      _ <- Resource.eval(
        supervisor.supervise(
          clients
            .map(socket => fs2.Stream.eval(secure(tls, socket, client = false).use(inbound.serve).handleErrorWith(e => log.debug(e)("Inbound connection closed"))))
            .parJoinUnbounded
            .compile
            .drain
        )
      )
      links   <- Resource.eval(Ref.of[F, Map[Address, Link[F]]](Map.empty))
      breaker <- Resource.eval(SignallingRef.of[F, Long](0L))
    } yield new TcpConnections[F](self, handshake, links, supervisor, outboxSize, network, tls, breaker, log)
  }

  private def tlsContext(network: Network[F], config: TlsConfig): F[TLSContext[F]] = A.blocking {
    def load(path: java.nio.file.Path, password: String): KeyStore = {
      val store = KeyStore.getInstance(if (path.toString.endsWith(".jks")) "JKS" else "PKCS12")
      val in = java.nio.file.Files.newInputStream(path)
      try store.load(in, password.toCharArray)
      finally in.close()
      store
    }
    val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keys.init(load(config.keyStore, config.keyStorePassword), config.keyPassword.getOrElse(config.keyStorePassword).toCharArray)
    val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trust.init(load(config.trustStore, config.trustStorePassword))
    val context = SSLContext.getInstance("TLS")
    context.init(keys.getKeyManagers, trust.getTrustManagers, new SecureRandom())
    network.tlsContext.fromSSLContext(context)
  }
}

object TcpTransport {

  /** Time without connecting to a node after a connection failed. */
  val ReconnectBackoff: FiniteDuration = 1.second

  /** Messages for a node that cannot be reached for this long are discarded. */
  val GiveUpAfter: FiniteDuration = 1.minute

  def apply[F[_]: Async]: TransportFactory[F] = new TcpTransport[F](outboxSize = 100000, ackInterval = 50.millis)

  /** Wraps the socket in TLS when configured, requiring the client's certificate on the server side. */
  private[transport] def secure[F[_]](tls: Option[TLSContext[F]], socket: Socket[F], client: Boolean): Resource[F, Socket[F]] =
    tls match {
      case None                 => Resource.pure(socket)
      case Some(ctx) if client  => ctx.clientBuilder(socket).build.widen[Socket[F]]
      case Some(ctx)            => ctx.serverBuilder(socket).withParameters(TLSParameters(needClientAuth = true)).build.widen[Socket[F]]
    }

  // ---- Wire protocol of a connection ----

  private[transport] sealed trait Frame
  private[transport] final case class Challenge(nonce: Array[Byte]) extends Frame
  private[transport] final case class Hello(from: Address, incarnation: Long, nonce: Array[Byte], proof: Array[Byte]) extends Frame
  private[transport] final case class Welcome(incarnation: Long, lastReceived: Long, proof: Array[Byte]) extends Frame
  private[transport] final case class Data(seq: Long, payload: Array[Byte]) extends Frame
  private[transport] final case class Ack(seq: Long) extends Frame

  private[transport] def encode(frame: Frame): Array[Byte] = {
    val w = BinWriter()
    frame match {
      case Challenge(nonce)                => w.byte(1).bytes(nonce)
      case Hello(from, inc, nonce, proof)  => w.byte(2).string(from.system).string(from.host).int(from.port).long(inc).bytes(nonce).bytes(proof)
      case Welcome(inc, last, proof)       => w.byte(3).long(inc).long(last).bytes(proof)
      case Data(seq, payload)              => w.byte(4).long(seq).bytes(payload)
      case Ack(seq)                        => w.byte(5).long(seq)
    }
    w.result()
  }

  private[transport] def decode(bytes: Array[Byte]): Frame = {
    val r = BinReader(bytes)
    r.byte() match {
      case 1     => Challenge(r.bytes())
      case 2     => Hello(Address(r.string(), r.string(), r.int()), r.long(), r.bytes(), r.bytes())
      case 3     => Welcome(r.long(), r.long(), r.bytes())
      case 4     => Data(r.long(), r.bytes())
      case 5     => Ack(r.long())
      case other => throw new IllegalStateException(s"unknown frame $other")
    }
  }

  /** Identity of this node and the secret both sides prove they know. */
  private[transport] final case class Handshake(self: Address, incarnation: Long, secret: Option[String]) {

    def nonce(): Array[Byte] = { val n = new Array[Byte](32); new SecureRandom().nextBytes(n); n }

    /** HMAC of the challenge with the secret; the role is mixed in so a proof cannot be echoed back. */
    def proof(role: String, challenge: Array[Byte]): Array[Byte] = secret.fold(Array.emptyByteArray) { s =>
      val mac = Mac.getInstance("HmacSHA256")
      mac.init(new SecretKeySpec(s.getBytes("UTF-8"), "HmacSHA256"))
      mac.update(role.getBytes("UTF-8"))
      mac.doFinal(challenge)
    }

    def verify(role: String, challenge: Array[Byte], proof: Array[Byte]): Boolean =
      secret.isEmpty || MessageDigest.isEqual(this.proof(role, challenge), proof)
  }

  private[transport] val HandshakeTimeout: FiniteDuration = 10.seconds

  /** Reads exactly one frame. */
  private[transport] def readFrame[F[_]](socket: Socket[F])(implicit A: Async[F]): F[Frame] =
    Frames.read(socket).take(1).compile.last.flatMap {
      case Some(bytes) => A.delay(decode(bytes))
      case None        => A.raiseError(new java.io.IOException("connection closed during the handshake"))
    }

  private[transport] def write[F[_]](socket: Socket[F], frame: Frame): F[Unit] = Frames.write(socket, encode(frame))

  // ---- Outgoing side ----

  /** Messages not yet acknowledged by the receiver, in order. */
  private[transport] final case class Outbox(nextSeq: Long, unacked: Vector[(Long, Array[Byte])], failingSince: Option[FiniteDuration])

  private[transport] final case class Link[F[_]](outbox: SignallingRef[F, Outbox])
}

/** Accepts connections: checks the handshake, delivers messages once and in order, and acknowledges them. */
private final class Inbound[F[_]](
    handshake: TcpTransport.Handshake,
    received: Ref[F, Map[Address, (Long, Long)]],
    onReceive: Array[Byte] => F[Unit],
    ackInterval: FiniteDuration,
    log: Log4Cats[F]
)(implicit A: Async[F]) {
  import TcpTransport._

  def serve(socket: Socket[F]): F[Unit] = {
    val challenge = handshake.nonce()
    val greet =
      write(socket, Challenge(challenge)) >> readFrame(socket).timeout(HandshakeTimeout).flatMap[Option[(Address, Long, Long)]] {
        case Hello(from, incarnation, nonce, proof) if handshake.verify("client", challenge, proof) =>
          // A new incarnation of the sender starts counting again.
          received
            .modify { r =>
              r.get(from) match {
                case Some((inc, last)) if inc == incarnation => (r, last)
                case _                                       => (r.updated(from, (incarnation, 0L)), 0L)
              }
            }
            .flatTap(last => write(socket, Welcome(handshake.incarnation, last, handshake.proof("server", nonce))))
            .map(last => Option((from, incarnation, last)))
        case Hello(from, _, _, _) => log.warn(s"Rejected a connection from $from: wrong shared secret").as(None)
        case other                => log.warn(s"Rejected a connection: unexpected $other").as(None)
      }

    greet.flatMap {
      case None => A.unit
      case Some((from, incarnation, _)) =>
        SignallingRef.of[F, Long](0L).flatMap { delivered =>
          val readLoop = Frames
            .read(socket)
            .evalMap { bytes =>
              A.delay(decode(bytes)).flatMap {
                case Data(seq, payload) =>
                  received
                    .modify { r =>
                      val last = r.get(from).filter(_._1 == incarnation).fold(0L)(_._2)
                      if (seq <= last) (r, false) else (r.updated(from, (incarnation, seq)), true)
                    }
                    .ifM(onReceive(payload).attempt.void >> delivered.set(seq), A.unit)
                case _ => A.unit
              }
            }
            .compile
            .drain

          // Acknowledges what was delivered, at most every `ackInterval`.
          val ackLoop = delivered.discrete.changes.metered(ackInterval).evalMap(seq => write(socket, Ack(seq))).compile.drain

          A.race(readLoop, ackLoop).void
        }
    }
  }
}

/** Outgoing side: one link per destination, each with its own connection loop. */
private[cluster] final class TcpConnections[F[_]](
    val address: Address,
    handshake: TcpTransport.Handshake,
    links: Ref[F, Map[Address, TcpTransport.Link[F]]],
    supervisor: Supervisor[F],
    outboxSize: Int,
    network: Network[F],
    tls: Option[TLSContext[F]],
    breaker: SignallingRef[F, Long],
    log: Log4Cats[F]
)(implicit A: Async[F])
    extends Transport[F] {
  import TcpTransport._

  /** Breaks every outgoing connection, as a network failure would. For tests. */
  private[cluster] def breakConnections: F[Unit] = breaker.update(_ + 1)

  def send(to: Address, payload: Array[Byte]): F[Unit] =
    link(to).flatMap { l =>
      l.outbox.modify { o =>
        val queued = o.unacked :+ (o.nextSeq -> payload)
        val dropped = queued.size - outboxSize
        (o.copy(nextSeq = o.nextSeq + 1, unacked = if (dropped > 0) queued.drop(dropped) else queued), dropped)
      }.flatMap(dropped => A.whenA(dropped > 0)(log.warn(s"Outbox to $to is full; dropped $dropped messages")))
    }

  private def link(to: Address): F[Link[F]] =
    links.get.map(_.get(to)).flatMap {
      case Some(l) => A.pure(l)
      case None =>
        SignallingRef.of[F, Outbox](Outbox(1L, Vector.empty, None)).flatMap { outbox =>
          val fresh = Link(outbox)
          links.modify(current => current.get(to).fold((current.updated(to, fresh), (fresh, true)))(existing => (current, (existing, false)))).flatMap {
            case (l, created) => A.whenA(created)(supervisor.supervise(connectionLoop(to, fresh)).void).as(l)
          }
        }
    }

  private def target(to: Address): F[SocketAddress[Host]] =
    A.fromOption(
      for {
        host <- Host.fromString(to.host)
        port <- Port.fromInt(to.port)
      } yield SocketAddress(host, port),
      new IllegalArgumentException(s"invalid address $to")
    )

  /** Connects while there is something to send, and reconnects after failures, until the destination is given up. */
  private def connectionLoop(to: Address, l: Link[F]): F[Unit] = {
    val waitForWork = l.outbox.discrete.find(_.unacked.nonEmpty).compile.drain

    val connectOnce: F[Unit] =
      target(to).flatMap { addr =>
        network.client(addr).flatMap(secure(tls, _, client = true)).use { socket =>
          for {
            challenge <- readFrame(socket).timeout(HandshakeTimeout).flatMap {
              case Challenge(nonce) => A.pure(nonce)
              case other            => A.raiseError[Array[Byte]](new java.io.IOException(s"unexpected $other"))
            }
            nonce = handshake.nonce()
            _ <- write(socket, Hello(address, handshake.incarnation, nonce, handshake.proof("client", challenge)))
            lastReceived <- readFrame(socket).timeout(HandshakeTimeout).flatMap {
              case Welcome(_, last, proof) if handshake.verify("server", nonce, proof) => A.pure(last)
              case Welcome(_, _, _) => A.raiseError[Long](new SecurityException(s"$to does not know the shared secret"))
              case other            => A.raiseError[Long](new java.io.IOException(s"unexpected $other"))
            }
            _ <- l.outbox.update(o => o.copy(unacked = o.unacked.dropWhile(_._1 <= lastReceived), failingSince = None))
            sent <- Ref.of[F, Long](lastReceived)
            // Sends everything not yet sent on this connection, including what is resent after a reconnection.
            writer = l.outbox.discrete
              .evalMap(o => sent.get.flatMap(s => o.unacked.filter(_._1 > s).traverse_ { case (seq, payload) => write(socket, Data(seq, payload)) >> sent.set(seq) }))
              .compile
              .drain
            reader = Frames
              .read(socket)
              .evalMap(bytes =>
                A.delay(decode(bytes)).flatMap {
                  case Ack(seq) => l.outbox.update(o => o.copy(unacked = o.unacked.dropWhile(_._1 <= seq)))
                  case _        => A.unit
                }
              )
              .compile
              .drain
            broken = breaker.discrete.drop(1).take(1).compile.drain >> A.raiseError[Unit](new java.io.IOException("connection broken"))
            _ <- A.race(A.race(writer, reader), broken)
          } yield ()
        }
      }

    def loop: F[Unit] =
      waitForWork >> connectOnce.attempt.flatMap { result =>
        Clock[F].monotonic.flatMap { now =>
          l.outbox.modify { o =>
            val since = o.failingSince.getOrElse(now)
            if (now - since >= GiveUpAfter) (Outbox(o.nextSeq, Vector.empty, None), Some(o.unacked.size))
            else (o.copy(failingSince = Some(since)), None)
          }.flatMap {
            case Some(dropped) => log.warn(s"$to unreachable for $GiveUpAfter; discarded $dropped messages")
            case None          => result.fold(e => log.debug(e)(s"Connection to $to failed"), _ => A.unit)
          }
        } >> A.sleep(ReconnectBackoff) >> loop
      }

    loop
  }
}
