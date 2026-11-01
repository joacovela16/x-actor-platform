package examples

// The examples of the README. They compile with the build, so the README stays correct.
// Run one with: sbt "examples/runMain examples.ActorsExample"

// ---------------------------------------------------------------- actors
object ActorsExample extends cats.effect.IOApp.Simple {
  import cats.effect.IO
  import xplat.core.api._
  import xplat.core.io.actor.IOActorSystemBuilder._

  sealed trait Counter
  case object Increment extends Counter
  final case class Get(replyTo: Ref[Int]) extends Counter

  // A behavior is a value: each message returns the behavior for the next one.
  def counter(b: Behaviors[IO, Counter], count: Int): Behavior[IO, Counter] =
    b.message {
      case Increment => counter(b, count + 1)
      case Get(replyTo) =>
        replyTo ! count
        b.same
    }

  def run: IO[Unit] =
    ActorSystem[IO].use { system =>
      for {
        ref   <- system.spawn[Counter]("counter")(b => counter(b, 0))
        _     <- ref ! Increment
        _     <- ref ! Increment
        count <- ref.ask[Int](Get(_))
        _     <- IO.println(s"count = $count") // count = 2
      } yield ()
    }
}

object BehaviorsExample {
  import cats.effect.IO
  import xplat.core.api._

  import scala.concurrent.duration._

  sealed trait Job
  case object Tick extends Job
  final case class Run(task: String) extends Job

  def worker: BehaviorBuilder[IO, Job] = b =>
    b.context { ctx =>
      // One-shot timer: sends Tick to this actor after 5 seconds.
      b.timer(5.seconds, Tick) { _ =>
        b.message {
          case Run(task) => ctx.log.info(s"running $task") // an IO[Unit] means "keep the same behavior"
          case Tick      => ctx.log.info("idle for 5s").as(b.stop)
        }
      }
    }.onSignal {
      case Signal.OnError(e) => IO.println(s"failed: ${e.getMessage}") // the actor keeps running
      case Signal.PostStop   => IO.println("stopped")
    }

  // Messages that cannot be handled yet are stashed and replayed later.
  def gate: BehaviorBuilder[IO, Job] = b =>
    b.stash(100) { stash =>
      def closed: Behavior[IO, Job] = b.message {
        case Tick         => stash.unstashAll(open)
        case job @ Run(_) => stash.push(job)
      }
      def open: Behavior[IO, Job] = b.message {
        case Run(task) => IO.println(s"run $task")
        case Tick      => b.same
      }
      closed
    }
}

// ---------------------------------------------------------------- persistence
object PersistenceExample extends cats.effect.IOApp.Simple {
  import cats.effect.IO
  import xplat.core.api._
  import xplat.core.api.error.CodecError
  import xplat.core.io.actor.IOActorSystemBuilder._
  import xplat.core.io.actor.IOPersistence._
  import xplat.core.persistence.Persistence
  import xplat.core.persistence.api.{Effects, JournalBuilder}
  import xplat.extensions.persistence.sqlite.SqlitePersistence

  import java.nio.charset.StandardCharsets.UTF_8
  import java.nio.file.Paths

  sealed trait AccountCmd
  final case class Deposit(amount: Long) extends AccountCmd
  final case class Balance(replyTo: Ref[Long]) extends AccountCmd
  case object TakeSnapshot extends AccountCmd

  final case class Deposited(amount: Long)

  // Commands decide which events to persist; events change the state.
  def onCommand(balance: Long, cmd: AccountCmd): IO[Effects[Deposited]] = cmd match {
    case Deposit(amount)  => IO.pure(Effects[Deposited].persist(Deposited(amount)))
    case Balance(replyTo) => IO(replyTo ! balance).as(Effects[Deposited])
    case TakeSnapshot     => IO.pure(Effects[Deposited].compress) // snapshot of the state
  }

  def onEvent(balance: Long, event: Deposited): Long = balance + event.amount

  // State and events are stored as bytes: use any serialization library (circe, protobuf, ...).
  implicit val longCodec: Codec[Long, Array[Byte]] = new Codec[Long, Array[Byte]] {
    def encode(from: Long): Either[CodecError, Array[Byte]] = Right(from.toString.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, Long] = new String(to, UTF_8).toLongOption.toRight(CodecError("not a number"))
  }
  implicit val depositedCodec: Codec[Deposited, Array[Byte]] = new Codec[Deposited, Array[Byte]] {
    def encode(from: Deposited): Either[CodecError, Array[Byte]] = longCodec.encode(from.amount)
    def decode(to: Array[Byte]): Either[CodecError, Deposited] = longCodec.decode(to).map(Deposited)
  }

  def run: IO[Unit] =
    SqlitePersistence.resource[IO](Paths.get("accounts.db")).use { db =>
      implicit val journals: JournalBuilder[IO] = db.journalBuilder
      val account = Persistence[IO, Long, AccountCmd, Deposited]("account", 0L, onCommand, onEvent)

      ActorSystem[IO].use { system =>
        for {
          // Events are stored under Shard("account", "acc-1"): restarting the actor replays them.
          ref     <- system.spawn[AccountCmd]("acc-1")(b => account(b))
          _       <- ref ! Deposit(100)
          balance <- ref.ask[Long](Balance(_))
          _       <- IO.println(s"balance = $balance") // grows by 100 on every run
        } yield ()
      }
    }
}

// ---------------------------------------------------------------- cluster
object ClusterExample {
  import cats.effect.{IO, Resource}
  import cats.implicits._
  import xplat.core.api.Codec
  import xplat.core.api.error.CodecError
  import xplat.core.cluster.Cluster
  import xplat.core.cluster.api._
  import xplat.core.io.actor.IOActorSystem
  import xplat.core.io.cluster.IOCluster
  import xplat.core.io.cluster.transport.TcpTransport

  import java.nio.charset.StandardCharsets.UTF_8

  implicit val clusterBuilder: ClusterBuilder[IO] = IOCluster.builder[IO](TcpTransport[IO])

  val seeds = List(Address("shop", "10.0.0.1", 2552), Address("shop", "10.0.0.2", 2552))

  def node(host: String): Resource[IO, Cluster[IO]] =
    for {
      system  <- IOActorSystem[IO]("shop") // every node uses the same system name
      cluster <- Cluster(system, ClusterConfig(host = host, port = 2552, seedNodes = seeds))
    } yield cluster

  implicit val textCodec: Codec[String, Array[Byte]] = new Codec[String, Array[Byte]] {
    def encode(from: String): Either[CodecError, Array[Byte]] = Right(from.getBytes(UTF_8))
    def decode(to: Array[Byte]): Either[CodecError, String] = Right(new String(to, UTF_8))
  }

  val LoggerKey = ServiceKey[String]("audit-logger")

  /** Starts the node on 10.0.0.1, registers an actor and prints the members as they join. */
  def start: IO[Unit] =
    node("10.0.0.1").use { cluster =>
      for {
        _      <- cluster.awaitUp
        logger <- cluster.system.spawn[String]("audit")(b => b.message(line => IO.println(line)))
        _      <- cluster.receptionist.register(LoggerKey, logger)
        // Any node can find the registered actors and send to them.
        found  <- cluster.receptionist.find(LoggerKey)
        _      <- found.services.toList.traverse_(_ ! "hello from 10.0.0.1")
        _ <- cluster.events
          .collect { case ClusterEvent.MemberUp(m) => m }
          .evalMap(m => IO.println(s"member up: ${m.address}"))
          .compile
          .drain
      } yield ()
    }
}

object SingletonExample {
  import cats.effect.IO
  import xplat.core.cluster.{Cluster, ClusterSingleton}
  import xplat.core.cluster.api._
  import xplat.core.io.cluster.IOClusterSingleton
  import ClusterExample.textCodec

  implicit val singletonBuilder: ClusterSingletonBuilder[IO] = IOClusterSingleton.builder[IO]

  def scheduler(cluster: Cluster[IO]): IO[Unit] =
    ClusterSingleton(cluster).use { singletons =>
      for {
        // Runs on the oldest node; moves to the next one when it leaves.
        ref <- singletons.init(SingletonActor[IO, String]("scheduler")(b => b.message(job => IO.println(s"scheduling $job"))))
        _   <- ref ! "nightly-report"
        _   <- IO.never[Unit]
      } yield ()
    }
}

// ---------------------------------------------------------------- sharding
object ShardingExample {
  import cats.effect.IO
  import xplat.core.api._
  import xplat.core.api.error.CodecError
  import xplat.core.cluster.Cluster
  import xplat.core.cluster.api.RefResolver
  import xplat.core.io.sharding.IOClusterSharding
  import xplat.core.sharding.ClusterSharding
  import xplat.core.sharding.api._
  import ClusterExample.textCodec

  import java.nio.charset.StandardCharsets.UTF_8

  sealed trait CartCmd
  final case class AddItem(sku: String) extends CartCmd
  final case class GetItems(replyTo: Ref[String]) extends CartCmd

  val CartKey = EntityTypeKey[CartCmd]("cart")

  // Messages may go to another node. Reply refs travel as text through the node's RefResolver.
  def cartCodec(resolver: RefResolver[IO]): Codec[CartCmd, Array[Byte]] = new Codec[CartCmd, Array[Byte]] {
    def encode(from: CartCmd): Either[CodecError, Array[Byte]] = Right((from match {
      case AddItem(sku)     => s"add\n$sku"
      case GetItems(replyTo) => s"get\n${resolver.serialize(replyTo)}"
    }).getBytes(UTF_8))

    def decode(to: Array[Byte]): Either[CodecError, CartCmd] = new String(to, UTF_8).split("\n", 2) match {
      case Array("add", sku) => Right(AddItem(sku))
      case Array("get", ref) => Right(GetItems(resolver.resolve[String](ref)))
      case _                 => Left(CodecError("unknown cart command"))
    }
  }

  def cart(b: Behaviors[IO, CartCmd], items: List[String]): Behavior[IO, CartCmd] =
    b.message {
      case AddItem(sku) => cart(b, sku :: items)
      case GetItems(replyTo) =>
        replyTo ! items.reverse.mkString(",")
        b.same
    }

  implicit val shardingBuilder: ClusterShardingBuilder[IO] = IOClusterSharding.builder[IO]()

  def run(cluster: Cluster[IO]): IO[Unit] =
    ClusterSharding(cluster).use { sharding =>
      val entity = Entity[IO, CartCmd](CartKey)(ctx => b => cart(b, Nil))(cartCodec(cluster.refResolver))
        .withConfig(ShardingConfig(role = Some("backend")))
      for {
        _     <- sharding.init(entity) // on every node that hosts or sends to carts
        cart   = sharding.entityRefFor(CartKey, "user-42")
        _     <- cart ! AddItem("book")
        items <- cart.ask[String](GetItems(_)) // the reply codec is implicit
        _     <- IO.println(items)
      } yield ()
    }
}

object ReliableDeliveryExample {
  import cats.effect.{IO, Resource}
  import xplat.core.api._
  import xplat.core.api.error.CodecError
  import xplat.core.cluster.api.RefResolver
  import xplat.core.io.sharding.delivery.{DeliveryCodec, Deduplicators, IOReliableProducer}
  import xplat.core.persistence.api.JournalBuilder
  import xplat.core.sharding.ClusterSharding
  import xplat.core.sharding.api._
  import xplat.core.sharding.delivery._
  import ClusterExample.textCodec

  final case class Notify(delivery: Delivery[String])

  val InboxKey = EntityTypeKey[Notify]("inbox")

  def inboxCodec(resolver: RefResolver[IO]): Codec[Notify, Array[Byte]] = new Codec[Notify, Array[Byte]] {
    private val deliveries = DeliveryCodec[IO, String](resolver)
    def encode(from: Notify): Either[CodecError, Array[Byte]] = deliveries.encode(from.delivery)
    def decode(to: Array[Byte]): Either[CodecError, Notify] = deliveries.decode(to).map(Notify)
  }

  // The consumer drops duplicates and confirms each delivery.
  def inbox(resolver: RefResolver[IO]): Entity[IO, Notify] =
    Entity[IO, Notify](InboxKey)(ctx => b =>
      b.context(_ =>
        Deduplicators.inMemory[IO].map { dedup =>
          b.message { case Notify(d) => dedup.handle(d)(text => IO.println(s"${ctx.entityId}: $text")) }
        }
      )
    )(inboxCodec(resolver))

  // Every message is stored before `send` returns and retried until the entity confirms it.
  def producer(sharding: ClusterSharding[IO], journals: JournalBuilder[IO]): Resource[IO, ReliableProducer[IO, String]] =
    IOReliableProducer.resource[IO, String, Notify]("mailer", sharding, InboxKey, journals)(Notify(_))
}

// ---------------------------------------------------------------- coordination
object LeaseExample {
  import cats.effect.{IO, Resource}
  import com.comcast.ip4s._
  import xplat.core.api.net.{TlsConfig, TransportSecurity}
  import xplat.core.cluster.api._
  import xplat.core.coordination.LeaseProvider
  import xplat.core.io.cluster.IOCluster
  import xplat.core.io.cluster.transport.TcpTransport
  import xplat.core.io.coordination.{InMemoryLeaseStore, LeaseServer, RemoteLeaseStore, StoreLeaseProvider}
  import xplat.extensions.persistence.postgres.PostgresPersistence
  import xplat.extensions.persistence.jdbc.JdbcConfig

  import java.nio.file.Paths

  val security = TransportSecurity(
    tls = Some(TlsConfig(Paths.get("node.p12"), "changeit", Paths.get("truststore.p12"), "changeit")),
    sharedSecret = Some(sys.env("CLUSTER_SECRET"))
  )

  // Option 1: leases in the database that also stores the journal.
  val fromDatabase: Resource[IO, LeaseProvider[IO]] =
    PostgresPersistence.resource[IO](JdbcConfig("jdbc:postgresql://db:5432/app", "app", "secret")).map(db => StoreLeaseProvider[IO](db.leaseStore))

  // Option 2: a small lease server, reached over TLS.
  val server: Resource[IO, SocketAddress[Host]] =
    Resource.eval(InMemoryLeaseStore[IO]).flatMap(store => LeaseServer.bind[IO](host"0.0.0.0", port"7355", store, security))
  val fromServer: Resource[IO, LeaseProvider[IO]] =
    RemoteLeaseStore[IO](SocketAddress(host"leases.internal", port"7355"), security).map(StoreLeaseProvider[IO](_))

  def clusterBuilder(leases: LeaseProvider[IO]): ClusterBuilder[IO] = IOCluster.builder[IO](TcpTransport[IO], Some(leases))

  val config = ClusterConfig(
    security = security,
    splitBrainResolver = Some(SplitBrainResolverConfig(SplitBrainStrategy.LeaseMajority("shop-sbr")))
  )
}

// ---------------------------------------------------------------- dependency injection
object DependencyInjectionExample extends cats.effect.IOApp.Simple {
  import cats.effect.{IO, Resource}
  import xplat.di._

  final case class Config(dbUrl: String)
  final class Database(val url: String)
  trait Plugin { def name: String }
  final class App(val db: Database, val plugins: Many[IO, Plugin])

  val ConfigKey = Key.single[Config]("config")
  val DatabaseKey = Key.single[Database]("database")
  val Plugins = Key.multiple[Plugin]("plugins")
  val AppKey = Key.single[App]("app")

  val S = Services[IO]

  // Each module exports its services as values; the composition stays in one place.
  val services = List(
    S(ConfigKey).pure(Config("jdbc:sqlite:app.db")),
    S(DatabaseKey).dependsOn(ConfigKey).resource(config => Resource.make(IO(new Database(config.dbUrl)))(_ => IO.println("db closed"))),
    S(Plugins).pure(new Plugin { val name = "audit" }),
    S(Plugins).dependsOn(DatabaseKey).pure(db => new Plugin { val name = s"backup of ${db.url}" }),
    S(AppKey).dependsOn(DatabaseKey, Plugins).pure((db, plugins) => new App(db, plugins))
  )

  def run: IO[Unit] =
    Container[IO].use { container =>
      for {
        _       <- container.provide(services: _*)
        app     <- container.get(AppKey)
        plugins <- app.plugins.current // live: plugins registered later show up here
        _       <- IO.println(plugins.map(_.name).mkString(", "))
      } yield ()
    } // releases the services in reverse order: prints "db closed"
}

// ---------------------------------------------------------------- messaging connectors
object ConnectorsExample {
  import cats.effect.{IO, Resource}
  import xplat.core.api._
  import xplat.core.connectors._
  import xplat.extensions.connectors.activemq.ActiveMQ
  import xplat.extensions.connectors.kafka.Kafka
  import xplat.extensions.connectors.redis.RedisStreams

  final case class Order(message: Incoming[String], replyTo: Ref[Done])

  // The actor replies Done once the order is processed; only then is the message acknowledged.
  def orders(b: Behaviors[IO, Order]): Behavior[IO, Order] =
    b.message { case Order(m, replyTo) => IO.println(s"order ${m.key.getOrElse("-")}: ${m.value}").map(_ => replyTo ! Done) }

  val kafka = Kafka.Config("localhost:9092")

  def run(system: ActorSystem[IO]): Resource[IO, Unit] =
    for {
      handler <- Resource.eval(system.spawn[Order]("orders")(b => orders(b)))
      // Kafka topic "orders" -> actor, committing each offset after the actor replies
      _ <- Kafka.consumer[IO, String](kafka, Kafka.ConsumerConfig(groupId = "billing", topics = List("orders")))(Sink.actor(handler)(Order(_, _)))
      // actor -> Kafka topic "invoices"
      invoices  <- Kafka.publisher[IO, String](kafka, "invoices")
      publisher <- Resource.eval(Publisher.spawn(system, "invoices", invoices))
      _         <- Resource.eval(publisher.ask[Publisher.Result](Publisher.Publish.confirmed(Outgoing("invoice-1", key = Some("customer-7")))))
      // the same with ActiveMQ and Redis Streams
      factory = ActiveMQ.artemis("tcp://localhost:61616", Some("app"), Some("secret"))
      _ <- ActiveMQ.consumer[IO, String](factory, ActiveMQ.Destination.Queue("orders"))(Sink.actor(handler)(Order(_, _)))
      _ <- RedisStreams.consumer[IO, String]("redis://localhost", RedisStreams.ConsumerConfig("orders", "billing", "node-1"))(Sink.actor(handler)(Order(_, _)))
    } yield ()

}
