package xplat.core.io.cluster

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api.net.{TlsConfig, TransportSecurity}
import xplat.core.cluster.api.Address
import xplat.core.io.cluster.ClusterTestKit.eventually
import xplat.core.io.cluster.transport.{TcpConnections, TcpTransport, Transport}

import java.net.ServerSocket
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths
import scala.concurrent.duration._

class TcpTransportTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private val Timeout = 30.seconds
  private val tcp = TcpTransport[IO]

  private def tlsFile(name: String) = Paths.get(getClass.getResource(s"/tls/$name").toURI)
  private def tls(keyStore: String): TlsConfig = TlsConfig(tlsFile(keyStore), "changeit", tlsFile("trust.p12"), "changeit")

  /** A transport that records what it receives. */
  private def endpoint(port: Int = 0, security: TransportSecurity = TransportSecurity()): Resource[IO, (Transport[IO], Ref[IO, Vector[String]])] =
    Resource.eval(Ref.of[IO, Vector[String]](Vector.empty)).flatMap { inbox =>
      tcp.bind("sys", "127.0.0.1", port, security, bytes => inbox.update(_ :+ new String(bytes, UTF_8))).map(_ -> inbox)
    }

  private def sendAll(from: Transport[IO], to: Address, messages: Seq[String]): IO[Unit] =
    messages.toList.traverse_(m => from.send(to, m.getBytes(UTF_8)))

  private def freePort: IO[Int] = IO.blocking { val s = new ServerSocket(0); try s.getLocalPort finally s.close() }

  private val numbers: Vector[String] = (1 to 500).map(_.toString).toVector

  "delivers every message once and in order" in {
    (endpoint(), endpoint()).tupled.use { case ((a, _), (b, inboxB)) =>
      sendAll(a, b.address, numbers) >> eventually(inboxB.get)(_.size >= numbers.size).map(_ shouldBe numbers)
    }.timeout(Timeout)
  }

  "resends what was not acknowledged after the connection breaks, without duplicates" in {
    (endpoint(), endpoint()).tupled.use { case ((a, _), (b, inboxB)) =>
      for {
        _     <- sendAll(a, b.address, numbers.take(250))
        _     <- a.asInstanceOf[TcpConnections[IO]].breakConnections
        _     <- sendAll(a, b.address, numbers.drop(250))
        _     <- a.asInstanceOf[TcpConnections[IO]].breakConnections
        got   <- eventually(inboxB.get)(_.size >= numbers.size)
        _     <- IO.sleep(500.millis)
        again <- inboxB.get
      } yield (got, again) shouldBe (numbers, numbers)
    }.timeout(Timeout)
  }

  "messages sent while the receiver is down arrive when it comes back" in {
    freePort.flatMap { port =>
      endpoint().use { case (a, _) =>
        val target = Address("sys", "127.0.0.1", port)
        sendAll(a, target, numbers.take(10)) >> IO.sleep(500.millis) >>
          endpoint(port).use { case (_, inbox) => eventually(inbox.get)(_.size >= 10).map(_ shouldBe numbers.take(10)) }
      }
    }.timeout(Timeout)
  }

  "accepts peers that know the shared secret" in {
    val secret = TransportSecurity(sharedSecret = Some("s3cret"))
    (endpoint(security = secret), endpoint(security = secret)).tupled.use { case ((a, _), (b, inboxB)) =>
      sendAll(a, b.address, numbers.take(5)) >> eventually(inboxB.get)(_.size >= 5).map(_ shouldBe numbers.take(5))
    }.timeout(Timeout)
  }

  "rejects peers with a different secret or none" in {
    val receiver = TransportSecurity(sharedSecret = Some("right"))
    (endpoint(security = receiver), endpoint(security = TransportSecurity(sharedSecret = Some("wrong"))), endpoint()).tupled.use {
      case ((b, inboxB), (wrong, _), (none, _)) =>
        sendAll(wrong, b.address, Seq("x")) >> sendAll(none, b.address, Seq("y")) >> IO.sleep(1.second) >> inboxB.get.map(_ shouldBe empty)
    }.timeout(Timeout)
  }

  "talks over mutual TLS with trusted certificates" in {
    val security = TransportSecurity(tls = Some(tls("node.p12")), sharedSecret = Some("s3cret"))
    (endpoint(security = security), endpoint(security = security)).tupled.use { case ((a, _), (b, inboxB)) =>
      sendAll(a, b.address, numbers.take(20)) >> eventually(inboxB.get)(_.size >= 20).map(_ shouldBe numbers.take(20))
    }.timeout(Timeout)
  }

  "rejects a client whose certificate is not trusted" in {
    (endpoint(security = TransportSecurity(tls = Some(tls("node.p12")))), endpoint(security = TransportSecurity(tls = Some(tls("rogue.p12"))))).tupled.use {
      case ((b, inboxB), (rogue, _)) =>
        sendAll(rogue, b.address, Seq("intruder")) >> IO.sleep(1.second) >> inboxB.get.map(_ shouldBe empty)
    }.timeout(Timeout)
  }
}
