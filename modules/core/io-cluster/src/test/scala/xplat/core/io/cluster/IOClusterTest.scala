package xplat.core.io.cluster

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref => CRef, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.cluster.api.ClusterEvent._
import xplat.core.cluster.api.SplitBrainStrategy.{KeepMajority, LeaseMajority}
import xplat.core.cluster.api._
import xplat.core.io.cluster.ClusterTestKit._
import xplat.core.io.cluster.transport.{InMemoryNetwork, TcpTransport}
import xplat.core.io.coordination.StoreLeaseProvider

import scala.concurrent.duration._

class IOClusterTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private val Timeout = 20.seconds

  private def threeNodes(network: InMemoryNetwork[IO], config: ClusterConfig = fastConfig): Resource[IO, List[Node]] = {
    val seeds = List(address(1))
    List(1, 2, 3).traverse(p => node(network, p, seeds, config))
  }

  "membership" - {

    "a single seed node forms a cluster and becomes Up and leader" in {
      InMemoryNetwork[IO].flatMap { network =>
        node(network, 1, List(address(1))).use { n =>
          for {
            _     <- n.cluster.awaitUp
            state <- n.cluster.state
            self  <- n.cluster.selfMember
          } yield (self.status, self.upNumber, state.leader) shouldBe (MemberStatus.Up, 1, Some(address(1)))
        }
      }.timeout(Timeout)
    }

    "nodes join through a seed and all become Up" in {
      InMemoryNetwork[IO].flatMap { network =>
        threeNodes(network).use { nodes =>
          for {
            _      <- allUp(nodes)
            states <- nodes.traverse(_.cluster.state)
          } yield {
            states.map(_.members.map(_.address)).distinct shouldBe List(Set(address(1), address(2), address(3)))
            states.map(_.leader).distinct shouldBe List(Some(address(1)))
            states.head.members.toList.map(_.upNumber).sorted shouldBe List(1, 2, 3)
          }
        }
      }.timeout(Timeout)
    }

    "join completes once the node is accepted" in {
      InMemoryNetwork[IO].flatMap { network =>
        (node(network, 1, List(address(1))), node(network, 2, Nil)).tupled.use { case (seed, other) =>
          seed.cluster.awaitUp >> other.cluster.join(List(address(1))) >> other.cluster.awaitUp >>
            other.cluster.selfMember.map(_.status shouldBe MemberStatus.Up)
        }
      }.timeout(Timeout)
    }

    "events start with the current state and then report changes" in {
      InMemoryNetwork[IO].flatMap { network =>
        node(network, 1, List(address(1))).use { seed =>
          for {
            _    <- seed.cluster.awaitUp
            seen <- CRef.of[IO, Vector[ClusterEvent]](Vector.empty)
            _    <- seed.cluster.events.evalMap(e => seen.update(_ :+ e)).compile.drain.start
            _    <- eventually(seen.get)(_.exists { case MemberUp(m) => m.address == address(1); case _ => false })
            _    <- node(network, 2, List(address(1))).use(n => n.cluster.awaitUp >> eventually(seen.get)(_.exists { case MemberUp(m) => m.address == address(2); case _ => false }))
            // Releasing the second node makes it leave: the seed sees it go.
            events <- eventually(seen.get)(_.exists { case MemberRemoved(m, _) => m.address == address(2); case _ => false })
          } yield events.collect { case e: MemberEvent if e.member.address == address(2) => e.getClass.getSimpleName } should contain inOrder ("MemberUp", "MemberLeft", "MemberRemoved")
        }
      }.timeout(Timeout)
    }

    "a node that leaves is removed and the others continue" in {
      InMemoryNetwork[IO].flatMap { network =>
        threeNodes(network).use { nodes =>
          val List(a, _, c) = nodes
          for {
            _      <- allUp(nodes)
            _      <- c.cluster.leave(address(3))
            _      <- eventually(statuses(a.cluster))(s => !s.contains(address(3)))
            _      <- c.cluster.asInstanceOf[IOCluster[IO]].awaitTerminated
            status <- c.cluster.selfMember.map(_.status)
          } yield status shouldBe MemberStatus.Removed
        }
      }.timeout(Timeout)
    }
  }

  "failure detection" - {

    "a bigger cluster converges and detects a failure with partial gossip and heartbeats" in {
      InMemoryNetwork[IO].flatMap { network =>
        (1 to 8).toList.traverse(p => node(network, p, List(address(1)))).use { nodes =>
          for {
            _ <- allUp(nodes)
            _ <- network.partition((1 to 7).map(address).toSet, Set(address(8)))
            _ <- nodes.take(7).traverse_(n => eventually(n.cluster.state)(_.unreachable.map(_.address) == Set(address(8))))
            _ <- nodes.head.cluster.down(address(8))
            _ <- nodes.take(7).traverse_(n => eventually(statuses(n.cluster))(s => s.size == 7 && s.values.forall(_ == MemberStatus.Up)))
          } yield succeed
        }
      }.timeout(Timeout)
    }

    "a cut-off node becomes unreachable, and reachable again when the network heals" in {
      InMemoryNetwork[IO].flatMap { network =>
        threeNodes(network).use { nodes =>
          val List(a, _, _) = nodes
          for {
            _ <- allUp(nodes)
            _ <- network.partition(Set(address(1), address(2)), Set(address(3)))
            _ <- eventually(a.cluster.state)(_.unreachable.map(_.address) == Set(address(3)))
            _ <- network.heal
            _ <- eventually(a.cluster.state)(_.unreachable.isEmpty)
          } yield succeed
        }
      }.timeout(Timeout)
    }

    "a downed node is removed and learns it was downed" in {
      InMemoryNetwork[IO].flatMap { network =>
        threeNodes(network).use { nodes =>
          val List(a, _, c) = nodes
          for {
            _ <- allUp(nodes)
            _ <- a.cluster.down(address(3))
            _ <- eventually(statuses(a.cluster))(s => !s.contains(address(3)))
            _ <- c.cluster.asInstanceOf[IOCluster[IO]].awaitTerminated
          } yield succeed
        }
      }.timeout(Timeout)
    }
  }

  "split brain resolver" - {

    val sbrConfig = fastConfig.copy(splitBrainResolver = Some(SplitBrainResolverConfig(KeepMajority(), stableAfter = 500.millis, downAllWhenUnstable = None)))

    "keep majority: the minority downs itself and the majority removes it" in {
      InMemoryNetwork[IO].flatMap { network =>
        threeNodes(network, sbrConfig).use { nodes =>
          val List(a, b, c) = nodes
          for {
            _ <- allUp(nodes)
            _ <- network.partition(Set(address(1), address(2)), Set(address(3)))
            _ <- c.cluster.asInstanceOf[IOCluster[IO]].awaitTerminated
            _ <- eventually(statuses(a.cluster))(_.keySet == Set(address(1), address(2)))
            _ <- eventually(statuses(b.cluster))(_.keySet == Set(address(1), address(2)))
          } yield succeed
        }
      }.timeout(Timeout)
    }

    "lease majority: on an even split exactly one side survives" in {
      val leaseConfig = fastConfig.copy(splitBrainResolver =
        Some(SplitBrainResolverConfig(LeaseMajority("sbr", acquireLeaseDelayForMinority = 300.millis), stableAfter = 500.millis, downAllWhenUnstable = None))
      )
      (InMemoryNetwork[IO], StoreLeaseProvider.inMemory[IO]).tupled.flatMap { case (network, leases) =>
        List(1, 2, 3, 4).traverse(p => node(network, p, List(address(1)), leaseConfig, Some(leases))).use { nodes =>
          val sideA = Set(address(1), address(2))
          val sideB = Set(address(3), address(4))
          for {
            _ <- allUp(nodes)
            _ <- network.partition(sideA, sideB)
            terminated <- eventually(nodes.traverse(n => n.cluster.asInstanceOf[IOCluster[IO]].isTerminated.map(n.address -> _)))(_.count(_._2) == 2)
            _          <- IO.sleep(1.second) // nothing else goes down
            stillDown  <- nodes.traverse(n => n.cluster.asInstanceOf[IOCluster[IO]].isTerminated.map(n.address -> _))
          } yield {
            val downed = stillDown.collect { case (a, true) => a }.toSet
            terminated.collect { case (a, true) => a }.toSet shouldBe downed
            Set(sideA, sideB) should contain(downed)
          }
        }
      }.timeout(Timeout)
    }
  }

  "receptionist" - {

    "finds actors on other nodes and delivers messages and asks to them" in {
      InMemoryNetwork[IO].flatMap { network =>
        (node(network, 1, List(address(1))), node(network, 2, List(address(1)))).tupled.use { case (a, b) =>
          val keyOnA = ServiceKey[Greet]("greeter")(greetCodec(a.cluster.refResolver))
          val keyOnB = ServiceKey[Greet]("greeter")(greetCodec(b.cluster.refResolver))
          for {
            _       <- allUp(List(a, b))
            greeter <- b.system.spawn[Greet]("greeter")(beh => beh.message(g => g.replyTo ! s"hello ${g.name}"))
            _       <- b.cluster.receptionist.register(keyOnB, greeter)
            listing <- eventually(a.cluster.receptionist.find(keyOnA))(_.services.size == 1)
            reply   <- listing.services.head.ask[String](Greet("ana", _))(stringCodec)
          } yield (listing.services.head.path, reply) shouldBe (s"${address(2)}/receptionist/greeter/xplat/sys/greeter", "hello ana")
        }
      }.timeout(Timeout)
    }

    "subscribers see registrations come and go" in {
      InMemoryNetwork[IO].flatMap { network =>
        (node(network, 1, List(address(1))), node(network, 2, List(address(1)))).tupled.use { case (a, b) =>
          val key = ServiceKey[String]("worker")(stringCodec)
          for {
            _       <- allUp(List(a, b))
            sizes   <- CRef.of[IO, Vector[Int]](Vector.empty)
            _       <- a.cluster.receptionist.subscribe(key).evalMap(l => sizes.update(_ :+ l.services.size)).compile.drain.start
            worker  <- b.system.spawn[String]("worker")(beh => beh.message(_ => beh.stop))
            _       <- b.cluster.receptionist.register(key, worker)
            _       <- eventually(sizes.get)(_.lastOption.contains(1))
            _       <- worker ! "stop" // a stopped actor is deregistered
            history <- eventually(sizes.get)(xs => xs.lastOption.contains(0) && xs.contains(1))
          } yield history.head shouldBe 0
        }
      }.timeout(Timeout)
    }

    "asks across nodes fail with a timeout when nobody answers" in {
      InMemoryNetwork[IO].flatMap { network =>
        (node(network, 1, List(address(1))), node(network, 2, List(address(1)))).tupled.use { case (a, b) =>
          val keyOnA = ServiceKey[Greet]("silent")(greetCodec(a.cluster.refResolver))
          val keyOnB = ServiceKey[Greet]("silent")(greetCodec(b.cluster.refResolver))
          for {
            _       <- allUp(List(a, b))
            silent  <- b.system.spawn[Greet]("silent")(beh => beh.message(_ => beh.same))
            _       <- b.cluster.receptionist.register(keyOnB, silent)
            listing <- eventually(a.cluster.receptionist.find(keyOnA))(_.services.nonEmpty)
            result  <- listing.services.head.ask[String](Greet("x", _), 200.millis)(stringCodec).attempt
          } yield result.left.map(_.getMessage) shouldBe Left("Ask timeout")
        }
      }.timeout(Timeout)
    }
  }

  "TCP transport" - {

    "a cluster over mutual TLS with a shared secret" in {
      def tlsFile(name: String) = java.nio.file.Paths.get(getClass.getResource(s"/tls/$name").toURI)
      val security = TransportSecurity(Some(TlsConfig(tlsFile("node.p12"), "changeit", tlsFile("trust.p12"), "changeit")), Some("s3cret"))
      val config = fastConfig.copy(security = security)
      val tcp = TcpTransport[IO]
      node(tcp, 0, Nil, config).use { seed =>
        node(tcp, 0, List(seed.address), config).use { other =>
          seed.cluster.join(List(seed.address)) >> allUp(List(seed, other)).as(succeed)
        }
      }.timeout(Timeout)
    }

    "nodes in a cluster over TCP see each other and exchange asks" in {
      val tcp = TcpTransport[IO]
      node(tcp, 0, Nil).use { seed =>
        node(tcp, 0, List(seed.address)).use { other =>
          val keyOnSeed = ServiceKey[Greet]("tcp-greeter")(greetCodec(seed.cluster.refResolver))
          val keyOnOther = ServiceKey[Greet]("tcp-greeter")(greetCodec(other.cluster.refResolver))
          for {
            _       <- seed.cluster.join(List(seed.address))
            _       <- allUp(List(seed, other))
            greeter <- other.system.spawn[Greet]("greeter")(beh => beh.message(g => g.replyTo ! s"hi ${g.name}"))
            _       <- other.cluster.receptionist.register(keyOnOther, greeter)
            listing <- eventually(seed.cluster.receptionist.find(keyOnSeed))(_.services.size == 1)
            reply   <- listing.services.head.ask[String](Greet("tcp", _))(stringCodec)
          } yield reply shouldBe "hi tcp"
        }
      }.timeout(Timeout)
    }
  }
}
