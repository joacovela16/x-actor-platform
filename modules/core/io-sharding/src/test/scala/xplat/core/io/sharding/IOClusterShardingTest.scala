package xplat.core.io.sharding

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Deferred, IO, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import fs2.Stream
import xplat.core.api.error.{CodecError, MailboxOverflow}
import xplat.core.api.{Behavior, Behaviors, Codec, Mailbox, ReplyTo}
import xplat.core.cluster.api.RefResolver
import xplat.core.coordination.LeaseUsage
import xplat.core.io.cluster.ClusterStream
import xplat.core.io.cluster.ClusterTestKit._
import xplat.core.io.cluster.transport.InMemoryNetwork
import xplat.core.io.coordination.{InMemoryLeaseStore, StoreLeaseProvider}
import xplat.core.sharding.ClusterSharding
import xplat.core.sharding.api._
import xplat.core.sharding.api.error.EntityTypeNotInitialized

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class IOClusterShardingTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import IOClusterShardingTest._

  private val Timeout = 40.seconds
  implicit private val shardingBuilder: ClusterShardingBuilder[IO] = IOClusterSharding.builder[IO]()

  private val fastSharding = ShardingConfig(passivateIdleAfter = None, rebalanceInterval = 300.millis, retryInterval = 100.millis, handOffTimeout = 2.seconds)

  private def sharded(
      network: InMemoryNetwork[IO],
      port: Int,
      config: ShardingConfig = fastSharding,
      roles: Set[String] = Set.empty,
      leases: Option[xplat.core.coordination.LeaseProvider[IO]] = None,
      onPassivate: IO[Unit] = IO.unit
  ): Resource[IO, ShardedNode] =
    for {
      n <- node(network, port, List(address(1)), fastConfig.copy(roles = roles), leases)
      s <- ClusterSharding[IO](n.cluster)
      _ <- Resource.eval(s.init(counter(n.address.port, n.cluster.refResolver, config, onPassivate)))
    } yield ShardedNode(n, s)

  private def get(s: ClusterSharding[IO], id: String): IO[String] =
    eventually(s.entityRefFor(Key, id).ask[String](Get(_), 1.second)(stringCodec).attempt)(_.isRight).map(_.toOption.get)

  private def hostedIds(nodes: Seq[ShardedNode]): IO[Map[Int, Set[String]]] =
    nodes.toList.traverse(n => n.sharding.regionState(Key).map(s => n.node.address.port -> s.entityIds)).map(_.toMap)

  "an entity with a full Fail mailbox rejects the send without blocking its shard" in {
    InMemoryNetwork[IO].flatMap { network =>
      node(network, 1, List(address(1)), fastConfig).flatMap(n => ClusterSharding[IO](n.cluster).map(n -> _)).use { case (n, sharding) =>
        for {
          _       <- allUp(List(n))
          started <- Deferred[IO, Unit]
          gate    <- Deferred[IO, Unit]
          _       <- sharding.init(gated(started, gate, n.cluster.refResolver))
          busy     = sharding.entityRefFor(GatedKey, "busy")
          _       <- busy ! Inc
          _       <- started.get // "busy" is handling its first message, so its mailbox is empty
          _       <- busy ! Inc
          full    <- (busy ! Inc).attempt
          other   <- sharding.entityRefFor(GatedKey, "other").ask[String](Get(_), 1.second)(stringCodec) // same shard
          _       <- gate.complete(())
          count   <- eventually(sharding.entityRefFor(GatedKey, "busy").ask[String](Get(_), 1.second)(stringCodec))(_ == "2")
        } yield {
          full shouldBe Left(MailboxOverflow("xplat/sys/cluster-sharding/gated-0/busy", 1))
          other shouldBe "0"
          count shouldBe "2"
        }
      }
    }.timeout(Timeout)
  }

  "ClusterStream sends a stream to an entity on another node" in {
    InMemoryNetwork[IO].flatMap { network =>
      List(1, 2).traverse(sharded(network, _)).use { nodes =>
        val List(a, b) = nodes
        val ref        = a.sharding.entityRefFor(Key, "streamed")
        for {
          _       <- allUp(nodes.map(_.node))
          _       <- get(b.sharding, "streamed") // wait until the entity is reachable
          _       <- Stream.emits(List.fill(5)(Inc: Cmd)).covary[IO].through(ClusterStream.sink(ref)).compile.drain
          _       <- eventually(get(b.sharding, "streamed"))(_.endsWith(":5"))
          answers <- Stream.range(0, 3).covary[IO].through(ClusterStream.ask(ref, parallelism = 2, 1.second)((_: Int, r: ReplyTo[String]) => Get(r))(implicitly, stringCodec)).compile.toList
        } yield answers.map(_.split(":").last) shouldBe List("5", "5", "5")
      }
    }.timeout(Timeout)
  }

  "delivers messages to an entity from any node and keeps its state" in {
    InMemoryNetwork[IO].flatMap { network =>
      List(1, 2, 3).traverse(sharded(network, _)).use { nodes =>
        val List(a, _, c) = nodes
        for {
          _     <- allUp(nodes.map(_.node))
          _     <- List.fill(3)(a.sharding.entityRefFor(Key, "acc-1") ! Inc).sequence_
          reply <- eventually(get(c.sharding, "acc-1"))(_.endsWith(":3"))
          where <- hostedIds(nodes).map(_.collect { case (port, ids) if ids("acc-1") => port })
        } yield (where.size, reply) shouldBe (1, s"${where.head}:3")
      }
    }.timeout(Timeout)
  }

  "spreads entities over the nodes" in {
    InMemoryNetwork[IO].flatMap { network =>
      List(1, 2, 3).traverse(sharded(network, _)).use { nodes =>
        for {
          _      <- allUp(nodes.map(_.node))
          ids     = (1 to 30).map(i => s"e-$i").toList
          _      <- ids.traverse_(id => get(nodes.head.sharding, id))
          hosted <- eventually(hostedIds(nodes))(_.values.flatten.size == ids.size)
        } yield {
          hosted.values.flatten.toSet shouldBe ids.toSet
          hosted.values.count(_.nonEmpty) should be > 1
        }
      }
    }.timeout(Timeout)
  }

  "an entity type that was not initialized is rejected" in {
    InMemoryNetwork[IO].flatMap { network =>
      sharded(network, 1).use { n =>
        n.sharding.entityRefFor(EntityTypeKey[Cmd]("unknown"), "x").send(Inc).attempt.map(_ shouldBe Left(EntityTypeNotInitialized("unknown")))
      }
    }.timeout(Timeout)
  }

  "the shard region ref accepts envelopes" in {
    InMemoryNetwork[IO].flatMap { network =>
      sharded(network, 1).use { n =>
        for {
          _      <- n.node.cluster.awaitUp
          region <- n.sharding.init(counter(1, n.node.cluster.refResolver, fastSharding))
          _      <- region ! ShardingEnvelope("env", Inc)
          reply  <- eventually(get(n.sharding, "env"))(_ == "1:1")
        } yield reply shouldBe "1:1"
      }
    }.timeout(Timeout)
  }

  "the region ref also carries asks" in {
    InMemoryNetwork[IO].flatMap { network =>
      sharded(network, 1).use { n =>
        for {
          _      <- n.node.cluster.awaitUp
          region <- n.sharding.init(counter(1, n.node.cluster.refResolver, fastSharding))
          _      <- region ! ShardingEnvelope("asked", Inc)
          reply  <- eventually(region.ask[String](r => ShardingEnvelope("asked", Get(r)), 1.second)(stringCodec).attempt)(_ == Right("1:1"))
        } yield reply shouldBe Right("1:1")
      }
    }.timeout(Timeout)
  }

  "with a durable coordinator, remembered entities restart after the whole cluster restarts" in {
    val remembering = fastSharding.copy(rememberEntities = true)
    (InMemoryNetwork[IO], SharedJournal()).tupled.flatMap { case (network, journal) =>
      val durable: ClusterShardingBuilder[IO] = IOClusterSharding.builder[IO](Some(journal.builder))
      def durableNode(port: Int): Resource[IO, ShardedNode] =
        for {
          n <- node(network, port, List(address(1)))
          s <- ClusterSharding[IO](n.cluster)(durable)
          _ <- Resource.eval(s.init(counter(n.address.port, n.cluster.refResolver, remembering)))
        } yield ShardedNode(n, s)

      val ids = (1 to 10).map(i => s"d-$i").toSet
      for {
        _ <- List(1, 2).traverse(durableNode).use { nodes =>
          allUp(nodes.map(_.node)) >> ids.toList.traverse_(id => get(nodes.head.sharding, id)) >>
            eventually(hostedIds(nodes))(_.values.flatten.toSet == ids) >> IO.sleep(500.millis) // let the regions report them
        }
        restarted <- List(1, 2).traverse(durableNode).use { nodes =>
          allUp(nodes.map(_.node)) >> eventually(hostedIds(nodes))(_.values.flatten.toSet == ids)
        }
      } yield restarted.values.flatten.toSet shouldBe ids
    }.timeout(Timeout)
  }

  "idle entities are passivated and start again with a fresh state" in {
    InMemoryNetwork[IO].flatMap { network =>
      sharded(network, 1, fastSharding.copy(passivateIdleAfter = Some(300.millis))).use { n =>
        for {
          _      <- n.node.cluster.awaitUp
          _      <- n.sharding.entityRefFor(Key, "idle") ! Inc
          _      <- eventually(get(n.sharding, "idle"))(_ == "1:1")
          _      <- eventually(n.sharding.regionState(Key))(!_.entityIds("idle"))
          again  <- get(n.sharding, "idle")
        } yield again shouldBe "1:0"
      }
    }.timeout(Timeout)
  }

  "an entity can passivate itself, and messages sent meanwhile are kept" in {
    (InMemoryNetwork[IO], Deferred[IO, Unit]).tupled.flatMap { case (network, passivating) =>
      sharded(network, 1, onPassivate = passivating.complete(()).void).use { n =>
        val ref = n.sharding.entityRefFor(Key, "self-passivate")
        for {
          _     <- n.node.cluster.awaitUp
          _     <- ref ! Inc
          _     <- eventually(get(n.sharding, "self-passivate"))(_ == "1:1")
          _     <- ref ! Passivate
          // A message sent before the passivation starts reaches the old entity and is lost with its state, so wait.
          _     <- passivating.get
          _     <- ref ! Inc
          reply <- eventually(get(n.sharding, "self-passivate"))(_ == "1:1")
        } yield reply shouldBe "1:1"
      }
    }.timeout(Timeout)
  }

  "entities move to another node when their node leaves" in {
    InMemoryNetwork[IO].flatMap { network =>
      sharded(network, 1).use { a =>
        for {
          alloc   <- sharded(network, 2).allocated
          (b, releaseB) = alloc
          _       <- allUp(List(a.node, b.node))
          ids      = (1 to 10).map(i => s"m-$i").toList
          _       <- ids.traverse_(id => get(a.sharding, id))
          onB     <- eventually(b.sharding.regionState(Key))(_.entityIds.nonEmpty).map(_.entityIds)
          _       <- releaseB
          replies <- onB.toList.traverse(id => eventually(get(a.sharding, id))(_.startsWith("1:")))
        } yield replies.forall(_.startsWith("1:")) shouldBe true
      }
    }.timeout(Timeout)
  }

  "remembered entities restart on another node without a message" in {
    val remembering = fastSharding.copy(rememberEntities = true)
    InMemoryNetwork[IO].flatMap { network =>
      sharded(network, 1, remembering).use { a =>
        for {
          alloc <- sharded(network, 2, remembering).allocated
          (b, releaseB) = alloc
          _     <- allUp(List(a.node, b.node))
          ids    = (1 to 10).map(i => s"r-$i").toList
          _     <- ids.traverse_(id => get(a.sharding, id))
          onB   <- eventually(b.sharding.regionState(Key))(_.entityIds.nonEmpty).map(_.entityIds)
          _     <- IO.sleep(300.millis) // let a registration report them
          _     <- releaseB
          onA   <- eventually(a.sharding.regionState(Key))(s => onB.subsetOf(s.entityIds))
        } yield onB.subsetOf(onA.entityIds) shouldBe true
      }
    }.timeout(Timeout)
  }

  "without a journal, a new coordinator takes over the remembered entities of a crashed node" in {
    val remembering = fastSharding.copy(rememberEntities = true)
    InMemoryNetwork[IO].flatMap { network =>
      List(1, 2, 3).traverse(sharded(network, _, remembering)).use { case nodes @ List(a, b, c) =>
        for {
          _   <- allUp(nodes.map(_.node))
          ids  = (1 to 12).map(i => s"c-$i").toList
          _   <- ids.traverse_(id => get(b.sharding, id))
          // Node 1 is the oldest, so it runs the coordinator, and hosts some of the entities.
          onA <- eventually(a.sharding.regionState(Key))(_.entityIds.nonEmpty).map(_.entityIds)
          _   <- IO.sleep(500.millis) // let the coordinator replicate its state
          _   <- network.partition(Set(a.node.address), Set(b.node.address, c.node.address))
          _   <- b.node.cluster.down(a.node.address)
          // Nobody sends them a message: only the replicated state says they existed.
          moved <- eventually(hostedIds(List(b, c)))(h => onA.subsetOf(h.values.flatten.toSet))
        } yield onA.subsetOf(moved.values.flatten.toSet) shouldBe true
      }
    }.timeout(Timeout)
  }

  "shards are rebalanced when nodes join" in {
    InMemoryNetwork[IO].flatMap { network =>
      sharded(network, 1).use { a =>
        val ids = (1 to 40).map(i => s"b-$i").toList
        for {
          _ <- a.node.cluster.awaitUp
          _ <- ids.traverse_(id => get(a.sharding, id))
          _ <- (sharded(network, 2), sharded(network, 3)).tupled.use { case (b, c) =>
            allUp(List(a.node, b.node, c.node)) >>
              // Moved shards start again on the next message.
              eventually(ids.traverse_(id => get(a.sharding, id)) >> hostedIds(List(a, b, c)))(_.values.forall(_.nonEmpty))
          }
        } yield succeed
      }
    }.timeout(Timeout)
  }

  "entities only run on nodes with the role" in {
    val backend = fastSharding.copy(role = Some("backend"))
    InMemoryNetwork[IO].flatMap { network =>
      (sharded(network, 1, backend), sharded(network, 2, backend, roles = Set("backend"))).tupled.use { case (front, back) =>
        val ids = (1 to 10).map(i => s"role-$i").toList
        for {
          _       <- allUp(List(front.node, back.node))
          replies <- ids.traverse(id => get(front.sharding, id))
          onFront <- front.sharding.regionState(Key)
        } yield (replies.map(_.takeWhile(_ != ':')).distinct, onFront.entityIds) shouldBe (List("2"), Set.empty)
      }
    }.timeout(Timeout)
  }

  "each shard holds a lease, when configured" in {
    InMemoryNetwork[IO].flatMap { network =>
      InMemoryLeaseStore[IO].flatMap { store =>
        val leased = fastSharding.copy(lease = Some(LeaseUsage()))
        sharded(network, 1, leased, leases = Some(StoreLeaseProvider[IO](store))).use { n =>
          val shard = ShardIdExtractor.hashCode(Entity.DefaultNumberOfShards).shardId("leased")
          for {
            _      <- n.node.cluster.awaitUp
            _      <- get(n.sharding, "leased")
            holder <- store.holder(s"sys-shard-counter-$shard")
          } yield holder shouldBe Some(address(1).toString)
        }
      }
    }.timeout(Timeout)
  }
}

object IOClusterShardingTest {

  final case class ShardedNode(node: Node, sharding: ClusterSharding[IO])

  sealed trait Cmd
  case object Inc extends Cmd
  case object Passivate extends Cmd
  final case class Get(replyTo: ReplyTo[String]) extends Cmd

  val Key: EntityTypeKey[Cmd] = EntityTypeKey[Cmd]("counter")

  def codec(resolver: RefResolver[IO]): Codec[Cmd, Array[Byte]] = new Codec[Cmd, Array[Byte]] {
    def encode(from: Cmd): Either[CodecError, Array[Byte]] = Right((from match {
      case Inc       => "inc"
      case Passivate => "passivate"
      case Get(r)    => s"get\n${resolver.serialize(r)}"
    }).getBytes(UTF_8))

    def decode(to: Array[Byte]): Either[CodecError, Cmd] = new String(to, UTF_8).split("\n", 2) match {
      case Array("inc")       => Right(Inc)
      case Array("passivate") => Right(Passivate)
      case Array("get", ref)  => Right(Get(resolver.resolve[String](ref)(stringCodec)))
      case other              => Left(CodecError(s"bad command ${other.mkString}"))
    }
  }

  /** Counter entity that answers `Get` with "<node port>:<count>". `onPassivate` runs once the shard is passivating it. */
  def counter(port: Int, resolver: RefResolver[IO], config: ShardingConfig, onPassivate: IO[Unit] = IO.unit): Entity[IO, Cmd] = {
    def running(b: Behaviors[IO, Cmd], ctx: EntityContext[IO, Cmd], count: Int): Behavior[IO, Cmd] =
      b.message {
        case Inc       => running(b, ctx, count + 1)
        case Passivate => (ctx.passivate >> onPassivate).as(b.same)
        case Get(replyTo) =>
          replyTo ! s"$port:$count"
          b.same
      }
    Entity[IO, Cmd](Key)(ctx => b => running(b, ctx, 0))(codec(resolver)).withConfig(config)
  }

  val GatedKey: EntityTypeKey[Cmd] = EntityTypeKey[Cmd]("gated")

  /** Counter whose mailbox holds one message. Its first `Inc` completes `started` and waits for `gate`. Every entity is
    * in shard "0", so a blocked shard would also block the other entities.
    */
  def gated(started: Deferred[IO, Unit], gate: Deferred[IO, Unit], resolver: RefResolver[IO]): Entity[IO, Cmd] = {
    def running(b: Behaviors[IO, Cmd], count: Int): Behavior[IO, Cmd] =
      b.message {
        case Inc if count == 0 => b.pipe(started.complete(()) >> gate.get.as(running(b, 1)))
        case Inc               => running(b, count + 1)
        case Passivate         => b.same
        case Get(replyTo) =>
          replyTo ! count.toString
          b.same
      }
    Entity[IO, Cmd](GatedKey)(_ => b => running(b, 0))(codec(resolver))
      .withShardIdExtractor(_ => "0")
      .withMailbox(Mailbox.bounded(1, Mailbox.Overflow.Fail))
  }
}
