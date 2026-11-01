package xplat.core.io.sharding

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref => CRef, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api.error.CodecError
import xplat.core.api.{Behavior, Behaviors, Codec, Ref}
import xplat.core.cluster.api.RefResolver
import xplat.core.io.cluster.ClusterTestKit._
import xplat.core.io.cluster.transport.InMemoryNetwork
import xplat.core.io.net.{BinReader, BinWriter}
import xplat.core.io.sharding.delivery.{DeliveryCodec, Deduplicators, IOReliableProducer}
import xplat.core.persistence.api.journal.Shard
import xplat.core.sharding.ClusterSharding
import xplat.core.sharding.api._
import xplat.core.sharding.delivery._

import scala.concurrent.duration._

class ReliableDeliveryTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import ReliableDeliveryTest._

  private val Timeout = 30.seconds
  implicit private val shardingBuilder: ClusterShardingBuilder[IO] = IOClusterSharding.builder[IO]()
  private val fastSharding = ShardingConfig(passivateIdleAfter = None, rebalanceInterval = 300.millis, retryInterval = 100.millis, handOffTimeout = 2.seconds)
  private val fastDelivery = ReliableProducerSettings(deliveryTimeout = 500.millis, retryInterval = 100.millis)

  private def read(s: ClusterSharding[IO], id: String): IO[List[String]] =
    s.entityRefFor(Key, id).ask[String](Read(_), 1.second)(stringCodec).map(_.split(",").toList.filter(_.nonEmpty))

  "a reliable producer" - {

    "delivers every message once and in order, even when confirmations are lost" in {
      (InMemoryNetwork[IO], SharedJournal()).tupled.flatMap { case (network, journal) =>
        val run = for {
          a        <- node(network, 1, List(address(1)))
          b        <- node(network, 2, List(address(1)))
          sa       <- ClusterSharding[IO](a.cluster)
          sb       <- ClusterSharding[IO](b.cluster)
          _        <- Resource.eval(List(a -> sa, b -> sb).traverse_ { case (n, s) => s.init(inbox(n.cluster.refResolver, loseFirstConfirmation = true)) })
          _        <- Resource.eval(allUp(List(a, b)))
          producer <- IOReliableProducer.resource[IO, String, InboxMsg]("p", sb, Key, journal.builder, fastDelivery)(Deliver(_))(textCodec, implicitly)
        } yield (sa, producer)

        run.use { case (sharding, producer) =>
          val sent = (1 to 12).toList.map(i => s"e-${i % 3}" -> s"m$i")
          for {
            _       <- sent.traverse_ { case (entity, msg) => producer.send(entity, msg) }
            _       <- eventually(producer.unconfirmed)(_ == 0)
            results <- List("e-0", "e-1", "e-2").traverse(id => read(sharding, id).map(id -> _))
          } yield results.toMap shouldBe sent.groupMap(_._1)(_._2)
        }
      }.timeout(Timeout)
    }

    "resumes the unconfirmed messages after it restarts, from a snapshot" in {
      val settings = fastDelivery.copy(snapshotEvery = 2)
      (InMemoryNetwork[IO], SharedJournal()).tupled.flatMap { case (network, journal) =>
        def producer(s: ClusterSharding[IO]) = IOReliableProducer.resource[IO, String, InboxMsg]("p", s, Key, journal.builder, settings)(Deliver(_))(textCodec, implicitly)
        val run = for {
          n <- node(network, 1, List(address(1)))
          s <- ClusterSharding[IO](n.cluster)
        } yield (n, s)

        run.use { case (n, s) =>
          for {
            _ <- n.cluster.awaitUp
            // The entity type is not running yet: nothing can be delivered.
            pending <- producer(s).use(p => (1 to 5).toList.traverse_(i => p.send("e", s"m$i")) >> IO.sleep(300.millis) >> p.unconfirmed)
            _       <- s.init(inbox(n.cluster.refResolver, loseFirstConfirmation = false))
            // A new producer with the same id delivers them.
            _        <- producer(s).use(p => eventually(p.unconfirmed)(_ == 0))
            snapshot <- journal.hasSnapshot(Shard("xplat-reliable-producer", "p"))
            // Restarting again finds nothing to send and keeps numbering after the last message.
            next     <- producer(s).use(p => (p.unconfirmed, p.send("e", "m6")).tupled <* eventually(p.unconfirmed)(_ == 0))
            received <- read(s, "e")
          } yield (pending, snapshot, next, received) shouldBe (5, true, (0, 6L), (1 to 6).map(i => s"m$i").toList)
        }
      }.timeout(Timeout)
    }
  }

  "deduplicators" - {

    "a journaled one drops what was processed before a restart, and confirms it again" in {
      val confirmed = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
      def delivery(seq: Long): Delivery[String] = Delivery("p", seq, s"m$seq", new Ref[Long] { def send(msg: Long): Unit = { confirmed.add(msg); () } })

      for {
        journal   <- SharedJournal()
        processed <- CRef.of[IO, List[String]](Nil)
        process    = (m: String) => processed.update(_ :+ m)
        first     <- Deduplicators.journaled[IO](journal.builder, "inbox", "e", snapshotEvery = 1)
        _         <- List(1L, 2L, 2L).traverse_(seq => first.handle(delivery(seq))(process))
        second    <- Deduplicators.journaled[IO](journal.builder, "inbox", "e")
        _         <- List(1L, 2L, 3L).traverse_(seq => second.handle(delivery(seq))(process))
        p         <- processed.get
        c         <- IO(scala.jdk.CollectionConverters.CollectionHasAsScala(confirmed).asScala.toList)
      } yield (p, c) shouldBe (List("m1", "m2", "m3"), List(1L, 2L, 2L, 1L, 2L, 3L))
    }

    "a failed process is not confirmed, so the producer sends it again" in {
      for {
        dedup     <- Deduplicators.inMemory[IO]
        confirmed <- CRef.of[IO, Int](0)
        d          = Delivery("p", 1L, "m", new Ref[Long] { def send(msg: Long): Unit = () })
        failed    <- dedup.handle(d)(_ => IO.raiseError(new RuntimeException("boom"))).attempt
        retried   <- dedup.handle(d)(_ => confirmed.update(_ + 1)).attempt
        count     <- confirmed.get
      } yield (failed.isLeft, retried.isRight, count) shouldBe (true, true, 1)
    }
  }
}

object ReliableDeliveryTest {

  sealed trait InboxMsg
  final case class Deliver(delivery: Delivery[String]) extends InboxMsg
  final case class Read(replyTo: Ref[String]) extends InboxMsg

  val Key: EntityTypeKey[InboxMsg] = EntityTypeKey[InboxMsg]("inbox")

  val textCodec: Codec[String, Array[Byte]] = stringCodec

  def codec(resolver: RefResolver[IO]): Codec[InboxMsg, Array[Byte]] = new Codec[InboxMsg, Array[Byte]] {
    private val deliveries = DeliveryCodec[IO, String](resolver)(textCodec)

    def encode(from: InboxMsg): Either[CodecError, Array[Byte]] = from match {
      case Deliver(d) => deliveries.encode(d).map(bytes => BinWriter().byte(1).bytes(bytes).result())
      case Read(r)    => Right(BinWriter().byte(2).string(resolver.serialize(r)).result())
    }

    def decode(to: Array[Byte]): Either[CodecError, InboxMsg] = {
      val r = BinReader(to)
      r.byte() match {
        case 1 => deliveries.decode(r.bytes()).map(Deliver)
        case 2 => Right(Read(resolver.resolve[String](r.string())(stringCodec)))
        case n => Left(CodecError(s"bad message $n"))
      }
    }
  }

  /** Entity that keeps the messages it receives. With `loseFirstConfirmation` the first confirmation of each message
    * is dropped, so the producer sends it again and the deduplicator has to discard the copy.
    */
  def inbox(resolver: RefResolver[IO], loseFirstConfirmation: Boolean): Entity[IO, InboxMsg] = {
    val noReply: Ref[Long] = new Ref[Long] { def send(msg: Long): Unit = () }

    def running(b: Behaviors[IO, InboxMsg], dedup: Deduplicator[IO], received: Vector[String], seen: Set[Long]): Behavior[IO, InboxMsg] =
      b.message {
        case Deliver(d) =>
          val firstTime = loseFirstConfirmation && !seen(d.seq)
          val delivery = if (firstTime) d.copy(confirmTo = noReply) else d
          CRef.of[IO, Vector[String]](received).flatMap { acc =>
            dedup.handle(delivery)(m => acc.update(_ :+ m)) >> acc.get.map(next => running(b, dedup, next, seen + d.seq))
          }
        case Read(replyTo) =>
          replyTo ! received.mkString(",")
          IO.pure(b.same)
      }

    Entity[IO, InboxMsg](Key)(_ => b => b.context(_ => Deduplicators.inMemory[IO].map(running(b, _, Vector.empty, Set.empty))))(codec(resolver))
      .withConfig(ShardingConfig(passivateIdleAfter = None, retryInterval = 100.millis))
  }
}
