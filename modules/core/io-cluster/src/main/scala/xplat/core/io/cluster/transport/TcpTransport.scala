package xplat.core.io.cluster.transport

import cats.effect.kernel.{Async, Clock, Ref, Resource}
import cats.effect.std.Supervisor
import cats.effect.syntax.all._
import cats.implicits._
import com.comcast.ip4s.{Host, Port, SocketAddress}
import fs2.concurrent.SignallingRef
import fs2.io.net.{Network, Socket}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger => Log4Cats}
import xplat.core.api.net.TransportSecurity
import xplat.core.cluster.api.Address
import xplat.core.io.net.{BinReader, BinWriter, Frames, SecureChannel}

import java.security.SecureRandom
import scala.concurrent.duration._

/** Transport over TCP, optionally with mutual TLS and a shared secret.
  *
  * Each node keeps one outgoing connection per destination. Every connection starts with the [[SecureChannel]]
  * handshake, in which both sides prove they know the shared secret (if configured) and exchange their identity.
  *
  * Delivery is reliable while both processes live: messages carry a sequence number, the receiver acknowledges them,
  * and the sender keeps the unacknowledged ones and sends them again after a reconnection, from the last one the
  * receiver reports. The receiver drops duplicates. A process restart is detected by its incarnation number, which
  * resets the sequence. Messages for a node that stays unreachable for [[TcpTransport.GiveUpAfter]] are discarded.
  * Messages that must survive a process restart go through a reliable producer of the sharding module, which stores
  * them until the receiving entity confirms them.
  */
final class TcpTransport[F[_]](outboxSize: Int, ackInterval: FiniteDuration)(implicit A: Async[F]) extends TransportFactory[F] {
  import TcpTransport._

  def bind(system: String, host: String, port: Int, security: TransportSecurity, onReceive: Array[Byte] => F[Unit]): Resource[F, Transport[F]] = {
    val network = Network.forAsync[F]
    for {
      log        <- Resource.eval(Slf4jLogger.fromName[F]("xplat.cluster.tcp"))
      bindHost   <- Resource.eval(A.fromOption(Host.fromString(host), new IllegalArgumentException(s"invalid host $host")))
      bindPort   <- Resource.eval(A.fromOption(Port.fromInt(port), new IllegalArgumentException(s"invalid port $port")))
      channel    <- Resource.eval(SecureChannel[F](network, security))
      supervisor <- Supervisor[F](await = false)
      server     <- network.serverResource(Some(bindHost), Some(bindPort))
      (bound, clients) = server
      self = Address(system, host, bound.port.value)
      incarnation <- Resource.eval(A.delay(new SecureRandom().nextLong()))
      received    <- Resource.eval(Ref.of[F, Map[Address, (Long, Long)]](Map.empty))
      inbound = new Inbound[F](channel, incarnation, received, onReceive, ackInterval, log)
      // Each connection is served inside the stream: a socket is closed when its stream element goes out of scope.
      _ <- Resource.eval(
        supervisor.supervise(
          clients
            .map(socket => fs2.Stream.eval(channel.server(socket).use(inbound.serve).handleErrorWith(e => log.debug(e)("Inbound connection closed"))))
            .parJoinUnbounded
            .compile
            .drain
        )
      )
      links   <- Resource.eval(Ref.of[F, Map[Address, Link[F]]](Map.empty))
      breaker <- Resource.eval(SignallingRef.of[F, Long](0L))
    } yield new TcpConnections[F](self, incarnation, channel, links, supervisor, outboxSize, network, breaker, log)
  }
}

object TcpTransport {

  /** Time without connecting to a node after a connection failed. */
  val ReconnectBackoff: FiniteDuration = 1.second

  /** Messages for a node that cannot be reached for this long are discarded. */
  val GiveUpAfter: FiniteDuration = 1.minute

  def apply[F[_]: Async]: TransportFactory[F] = new TcpTransport[F](outboxSize = 100000, ackInterval = 50.millis)

  // ---- Wire protocol of a connection ----

  /** What the client says in the handshake: who it is. */
  private[transport] def hello(from: Address, incarnation: Long): Array[Byte] =
    BinWriter().string(from.system).string(from.host).int(from.port).long(incarnation).result()

  private[transport] def readHello(bytes: Array[Byte]): (Address, Long) = {
    val r = BinReader(bytes)
    (Address(r.string(), r.string(), r.int()), r.long())
  }

  /** What the server answers: the last message it delivered from that client, so the client resends from there. */
  private[transport] def welcome(lastReceived: Long): Array[Byte] = BinWriter().long(lastReceived).result()

  private[transport] def readWelcome(bytes: Array[Byte]): Long = BinReader(bytes).long()

  private[transport] sealed trait Frame
  private[transport] final case class Data(seq: Long, payload: Array[Byte]) extends Frame
  private[transport] final case class Ack(seq: Long) extends Frame

  private[transport] def encode(frame: Frame): Array[Byte] = {
    val w = BinWriter()
    frame match {
      case Data(seq, payload) => w.byte(4).long(seq).bytes(payload)
      case Ack(seq)           => w.byte(5).long(seq)
    }
    w.result()
  }

  private[transport] def decode(bytes: Array[Byte]): Frame = {
    val r = BinReader(bytes)
    r.byte() match {
      case 4     => Data(r.long(), r.bytes())
      case 5     => Ack(r.long())
      case other => throw new IllegalStateException(s"unknown frame $other")
    }
  }

  private[transport] def write[F[_]](socket: Socket[F], frame: Frame): F[Unit] = Frames.write(socket, encode(frame))

  // ---- Outgoing side ----

  /** Messages not yet acknowledged by the receiver, in order. */
  private[transport] final case class Outbox(nextSeq: Long, unacked: Vector[(Long, Array[Byte])], failingSince: Option[FiniteDuration])

  private[transport] final case class Link[F[_]](outbox: SignallingRef[F, Outbox])
}

/** Accepts connections: checks the handshake, delivers messages once and in order, and acknowledges them. */
private final class Inbound[F[_]](
    channel: SecureChannel[F],
    incarnation: Long,
    received: Ref[F, Map[Address, (Long, Long)]],
    onReceive: Array[Byte] => F[Unit],
    ackInterval: FiniteDuration,
    log: Log4Cats[F]
)(implicit A: Async[F]) {
  import TcpTransport._

  def serve(socket: Socket[F]): F[Unit] = {
    // A new incarnation of the sender starts counting again.
    def lastFrom(from: Address, incarnation: Long): F[Long] =
      received.modify { r =>
        r.get(from) match {
          case Some((inc, last)) if inc == incarnation => (r, last)
          case _                                       => (r.updated(from, (incarnation, 0L)), 0L)
        }
      }

    val greet: F[Option[(Address, Long)]] =
      channel
        .accept(socket)(payload => A.delay(readHello(payload)).flatMap { case (from, inc) => lastFrom(from, inc).map(welcome) })
        .flatMap {
          case Some(payload) => A.delay(Option(readHello(payload)))
          case None          => log.warn("Rejected a connection: the client does not know the shared secret").as(None)
        }

    greet.flatMap {
      case None => A.unit
      case Some((from, incarnation)) =>
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
    incarnation: Long,
    channel: SecureChannel[F],
    links: Ref[F, Map[Address, TcpTransport.Link[F]]],
    supervisor: Supervisor[F],
    outboxSize: Int,
    network: Network[F],
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
        network.client(addr).flatMap(channel.client).use { socket =>
          for {
            lastReceived <- channel.connect(socket, hello(address, incarnation)).flatMap(answer => A.delay(readWelcome(answer)))
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
