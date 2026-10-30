package xplat.core.io.cluster

import cats.effect.{IO, Resource}
import cats.implicits._
import xplat.core.api.error.CodecError
import xplat.core.api.{ActorSystem, Codec, Ref}
import xplat.core.cluster.Cluster
import xplat.core.cluster.api._
import xplat.core.coordination.LeaseProvider
import xplat.core.io.actor.IOActorSystem
import xplat.core.io.cluster.transport.TransportFactory

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

object ClusterTestKit {

  /** Short intervals so tests run fast. */
  val fastConfig: ClusterConfig = ClusterConfig(
    host = "127.0.0.1",
    gossipInterval = 50.millis,
    retryInterval = 100.millis,
    heartbeatInterval = 50.millis,
    failureDetector = FailureDetectorConfig(acceptableHeartbeatPause = 300.millis, minStdDeviation = 50.millis)
  )

  def address(port: Int): Address = Address("sys", "127.0.0.1", port)

  final case class Node(system: ActorSystem[IO], cluster: Cluster[IO]) {
    def address: Address = cluster.asInstanceOf[IOCluster[IO]].self.address
  }

  def node(
      transport: TransportFactory[IO],
      port: Int,
      seeds: List[Address],
      config: ClusterConfig = fastConfig,
      leases: Option[LeaseProvider[IO]] = None
  ): Resource[IO, Node] =
    for {
      system  <- IOActorSystem[IO]("sys")
      cluster <- Cluster[IO](system, config.copy(port = port, seedNodes = seeds))(IOCluster.builder[IO](transport, leases))
    } yield Node(system, cluster)

  /** Re-evaluates `io` until its result satisfies `p`. */
  def eventually[A](io: IO[A])(p: A => Boolean): IO[A] =
    io.flatMap(a => if (p(a)) IO.pure(a) else IO.sleep(25.millis) >> eventually(io)(p))

  def statuses(cluster: Cluster[IO]): IO[Map[Address, MemberStatus]] =
    cluster.state.map(_.members.map(m => m.address -> m.status).toMap)

  def allUp(nodes: Seq[Node]): IO[Unit] =
    nodes.toList.traverse_(n => eventually(statuses(n.cluster))(s => s.size == nodes.size && s.values.forall(_ == MemberStatus.Up)))

  val stringCodec: Codec[String, Array[Byte]] = new Codec[String, Array[Byte]] {
    def encode(from: String): Either[CodecError, Array[Byte]] = Right(from.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, String] = Right(new String(to, UTF_8))
  }

  /** Message with a reply ref, as used with `ask`. */
  final case class Greet(name: String, replyTo: Ref[String])

  /** The ref travels as text, through the resolver of the node that encodes or decodes it. */
  def greetCodec(resolver: RefResolver[IO]): Codec[Greet, Array[Byte]] = new Codec[Greet, Array[Byte]] {
    def encode(from: Greet): Either[CodecError, Array[Byte]] =
      Right(s"${from.name}\n${resolver.serialize(from.replyTo)}".getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, Greet] =
      new String(to, UTF_8).split("\n", 2) match {
        case Array(name, ref) => Right(Greet(name, resolver.resolve[String](ref)(stringCodec)))
        case _                => Left(CodecError("bad greet"))
      }
  }
}
