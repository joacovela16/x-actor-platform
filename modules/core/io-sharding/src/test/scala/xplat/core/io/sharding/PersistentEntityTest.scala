package xplat.core.io.sharding

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref => CRef, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api.Codec
import xplat.core.api.error.CodecError
import xplat.core.io.actor.IOPersistence
import xplat.core.io.cluster.ClusterTestKit._
import xplat.core.io.cluster.transport.InMemoryNetwork
import xplat.core.io.sharding.IOClusterShardingTest._
import xplat.core.persistence.Persistence
import xplat.core.persistence.api.journal.{JournalEvent, JournalSnapshot, Shard}
import xplat.core.persistence.api.{Effect, Effects, Journal, JournalBuilder}
import xplat.core.sharding.ClusterSharding
import xplat.core.sharding.api._

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class PersistentEntityTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  implicit private val shardingBuilder: ClusterShardingBuilder[IO] = IOClusterSharding.builder[IO]

  /** Journal shared by every node, standing in for a database. */
  final class SharedJournal(events: CRef[IO, Map[Shard, Vector[Array[Byte]]]]) extends Journal[IO] {
    def create(shard: Shard): IO[Unit] = IO.unit
    def getEvents(shard: Shard): IO[fs2.Stream[IO, JournalEvent]] =
      events.get.map(m => fs2.Stream.emits(m.getOrElse(shard, Vector.empty).zipWithIndex.map { case (d, i) => JournalEvent(shard.name, shard.id, i.toLong, 0L, d) }))
    def getSnapshot(shard: Shard): IO[Option[JournalSnapshot]] = IO.pure(None)
    def writeSnapshot(shard: Shard, data: Array[Byte]): IO[Unit] = IO.unit
    def writeEvents(shard: Shard, entries: Seq[Array[Byte]]): IO[Unit] = events.update(m => m.updated(shard, m.getOrElse(shard, Vector.empty) ++ entries))
    def close(): IO[Unit] = IO.unit
  }

  implicit private val intCodec: Codec[Int, Array[Byte]] = new Codec[Int, Array[Byte]] {
    def encode(from: Int): Either[CodecError, Array[Byte]] = Right(from.toString.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, Int] = new String(to, UTF_8).toIntOption.toRight(CodecError("not a number"))
  }

  private def persistentCounter(port: Int, n: Node, journal: Journal[IO], config: ShardingConfig): Entity[IO, Cmd] = {
    implicit val journals: JournalBuilder[IO] = () => IO.pure(journal)
    val persistence = IOPersistence.mkIOPersistence[IO]
    val handler: (Int, Cmd) => IO[Effects[Int]] = {
      case (_, Inc)          => IO.pure(Effects(Seq(Effect.Persist(1))))
      case (_, Passivate)    => IO.pure(Effects(Nil))
      case (count, Get(ref)) => IO(ref ! s"$port:$count").as(Effects(Nil))
    }
    Entity[IO, Cmd](Key)(_ => persistence.build[Int, Cmd, Int]("counter", 0, handler, _ + _, Persistence.Config(shutdownOnIdle = None)))(codec(n.cluster.refResolver))
      .withConfig(config)
  }

  "a persistent entity keeps its state when its node leaves" in {
    val config = ShardingConfig(passivateIdleAfter = None, rebalanceInterval = 300.millis, retryInterval = 100.millis)
    (InMemoryNetwork[IO], CRef.of[IO, Map[Shard, Vector[Array[Byte]]]](Map.empty)).tupled.flatMap { case (network, store) =>
      val journal = new SharedJournal(store)
      def sharded(port: Int): Resource[IO, (Node, ClusterSharding[IO])] =
        for {
          n <- node(network, port, List(address(1)))
          s <- ClusterSharding[IO](n.cluster)
          _ <- Resource.eval(s.init(persistentCounter(port, n, journal, config)))
        } yield (n, s)

      def get(s: ClusterSharding[IO], id: String): IO[String] =
        eventually(s.entityRefFor(Key, id).ask[String](Get(_), 1.second)(stringCodec).attempt)(_.isRight).map(_.toOption.get)

      sharded(1).use { case (a, shardingA) =>
        for {
          alloc <- sharded(2).allocated
          ((b, shardingB), releaseB) = alloc
          _     <- allUp(List(a, b))
          ids    = (1 to 10).map(i => s"p-$i").toList
          _     <- ids.traverse_(id => List.fill(3)(shardingA.entityRefFor(Key, id) ! Inc).sequence_)
          _     <- ids.traverse_(id => eventually(get(shardingA, id))(_.endsWith(":3")))
          onB   <- shardingB.regionState(Key).map(_.entityIds)
          _     <- releaseB
          after <- onB.toList.traverse(id => eventually(get(shardingA, id))(_.startsWith("1:")))
        } yield {
          onB should not be empty
          after.distinct shouldBe List("1:3")
        }
      }
    }.timeout(40.seconds)
  }
}
