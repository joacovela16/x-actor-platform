package xplat.core.io.cluster

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref, Resource}
import cats.implicits._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.core.api.{Behavior, Behaviors}
import xplat.core.cluster.ClusterSingleton
import xplat.core.cluster.api._
import xplat.core.coordination.LeaseUsage
import xplat.core.io.cluster.ClusterTestKit._
import xplat.core.io.cluster.transport.InMemoryNetwork
import xplat.core.io.coordination.{InMemoryLeaseStore, StoreLeaseProvider}

import scala.concurrent.duration._

class IOClusterSingletonTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  private val Timeout = 30.seconds
  implicit private val singletons: ClusterSingletonBuilder[IO] = IOClusterSingleton.builder[IO]

  /** A singleton that answers with the node it runs on. Each node defines it with its own resolver. */
  private def whereAmI(n: Node, config: SingletonConfig = SingletonConfig(handOverRetryInterval = 100.millis), stopMessage: Option[Greet] = None): SingletonActor[IO, Greet] =
    SingletonActor[IO, Greet]("where", Behaviors.message(g => g.replyTo ! s"${n.address.port}:${g.name}"): Behavior[IO, Greet], config, stopMessage)(
      greetCodec(n.cluster.refResolver)
    )

  private def withSingleton(n: Node, singleton: SingletonActor[IO, Greet]): Resource[IO, ClusterRef[IO, Greet]] =
    ClusterSingleton(n.cluster).evalMap(_.init(singleton))

  private def where(ref: ClusterRef[IO, Greet]): IO[String] = ref.ask[String](Greet("q", _), 1.second)(stringCodec)

  "runs on the oldest node and every node reaches it" in {
    InMemoryNetwork[IO].flatMap { network =>
      List(1, 2, 3).traverse(p => node(network, p, List(address(1)))).use { nodes =>
        allUp(nodes) >>
          nodes.traverse(n => withSingleton(n, whereAmI(n))).use { refs =>
            refs.traverse(ref => eventually(where(ref).attempt)(_.isRight).map(_.toOption.get)).map(_ shouldBe List("1:q", "1:q", "1:q"))
          }
      }
    }.timeout(Timeout)
  }

  "moves to the next oldest node when the oldest leaves" in {
    InMemoryNetwork[IO].flatMap { network =>
      node(network, 2, List(address(1))).use { second =>
        val first = node(network, 1, List(address(1)))
        for {
          alloc1 <- first.allocated
          (firstNode, releaseFirst) = alloc1
          _                         <- allUp(List(firstNode, second))
          alloc2 <- withSingleton(firstNode, whereAmI(firstNode)).allocated
          (_, releaseSingleton1) = alloc2
          ref                       <- withSingleton(second, whereAmI(second)).allocated.map(_._1)
          before                    <- eventually(where(ref).attempt)(_.isRight)
          _                         <- releaseSingleton1 >> releaseFirst
          after                     <- eventually(where(ref).attempt)(_ == Right("2:q"))
        } yield (before, after) shouldBe (Right("1:q"), Right("2:q"))
      }
    }.timeout(Timeout)
  }

  "sends the stop message before moving" in {
    InMemoryNetwork[IO].flatMap { network =>
      Ref.of[IO, Vector[String]](Vector.empty).flatMap { log =>
        def recording(n: Node): SingletonActor[IO, Greet] =
          SingletonActor[IO, Greet](
            "recording",
            (Behaviors.message(g => if (g.name == "stop") Behaviors.pipe(log.update(_ :+ s"stop@${n.address.port}").as(Behaviors.stop)) else IO(g.replyTo ! "ok").void)): Behavior[IO, Greet], // typed: SingletonActor.apply is overloaded
            SingletonConfig(handOverRetryInterval = 100.millis),
            stopMessage = Some(Greet("stop", null))
          )(greetCodec(n.cluster.refResolver))

        node(network, 2, List(address(1))).use { second =>
          for {
            alloc3 <- node(network, 1, List(address(1))).allocated
            (firstNode, releaseFirst) = alloc3
            _                         <- allUp(List(firstNode, second))
            alloc4 <- ClusterSingleton(firstNode.cluster).evalMap(_.init(recording(firstNode))).allocated
            (_, releaseSingleton1) = alloc4
            ref                       <- ClusterSingleton(second.cluster).evalMap(_.init(recording(second))).allocated.map(_._1)
            _                         <- eventually(ref.ask[String](Greet("q", _), 1.second)(stringCodec).attempt)(_.isRight)
            _                         <- releaseSingleton1 >> releaseFirst
            stops                     <- eventually(log.get)(_.nonEmpty)
          } yield stops shouldBe Vector("stop@1")
        }
      }
    }.timeout(Timeout)
  }

  "does not start a second copy while the old owner is only unreachable" in {
    InMemoryNetwork[IO].flatMap { network =>
      List(1, 2, 3).traverse(p => node(network, p, List(address(1)))).use { nodes =>
        val List(a, b, _) = nodes
        allUp(nodes) >>
          nodes.traverse(n => withSingleton(n, whereAmI(n))).use { refs =>
            for {
              _        <- eventually(where(refs(1)).attempt)(_ == Right("1:q"))
              _        <- network.partition(Set(address(1)), Set(address(2), address(3)))
              _        <- eventually(b.cluster.state)(_.unreachable.map(_.address) == Set(address(1)))
              _        <- IO.sleep(1.second)
              whileCut <- where(refs(1)).attempt
              _        <- b.cluster.down(address(1))
              after    <- eventually(where(refs(1)).attempt)(_ == Right("2:q"))
            } yield whileCut.isLeft shouldBe true
          }
      }
    }.timeout(Timeout)
  }

  "holds a lease while it runs, when configured" in {
    InMemoryNetwork[IO].flatMap { network =>
      InMemoryLeaseStore[IO].flatMap { store =>
        val leases = StoreLeaseProvider[IO](store)
        node(network, 1, List(address(1)), leases = Some(leases)).use { n =>
          val config = SingletonConfig(handOverRetryInterval = 100.millis, lease = Some(LeaseUsage(retryInterval = 100.millis)))
          n.cluster.awaitUp >> withSingleton(n, whereAmI(n, config)).use { ref =>
            for {
              _      <- eventually(where(ref).attempt)(_.isRight)
              holder <- store.holder("sys-singleton-where")
            } yield holder shouldBe Some(address(1).toString)
          }
        }
      }
    }.timeout(Timeout)
  }
}
