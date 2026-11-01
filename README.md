# x-actor-platform

[![Maven Central](https://img.shields.io/maven-central/v/io.github.joacovela16/xplat-io-actor_2.13)](https://central.sonatype.com/namespace/io.github.joacovela16)
[![Scaladex](https://index.scala-lang.org/joacovela16/x-actor-platform/xplat-io-actor/latest.svg)](https://index.scala-lang.org/joacovela16/x-actor-platform)
[![CI](https://github.com/joacovela16/x-actor-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/joacovela16/x-actor-platform/actions/workflows/ci.yml)

Typed actors for [cats-effect](https://typelevel.org/cats-effect/), with the building blocks to run them on a cluster:

- **Actors**: typed behaviors, `ask`, child actors, timers, stash and signals, all as `F[_]` effects.
- **Event sourcing**: persistent actors with snapshots, stored in SQLite, PostgreSQL, MariaDB or MongoDB.
- **Cluster**: membership with gossip, a phi accrual failure detector, split brain resolver and a receptionist to find actors on any node.
- **Cluster singleton**: one actor in the whole cluster, moved to another node when its node leaves.
- **Sharding**: entities spread over the nodes, reachable by id from anywhere, with passivation, rebalancing and remembered entities.
- **Reliable delivery**: messages to sharded entities that survive restarts, delivered at least once and in order.
- **Coordination**: leases backed by a database or a lease server, to keep singletons and shards from running twice.
- **Secure transport**: TCP between nodes with mutual TLS and a shared secret.

Every feature is split into a small API module (traits and configuration) and an implementation on cats-effect, so your
code depends on the API and picks the implementation at the edge.

## Installation

Published for Scala 2.13. Replace `<version>` with the latest release shown in the badge above.

```scala
val xplatVersion = "<version>"

libraryDependencies ++= Seq(
  // Actors, cluster, singletons and sharding (pulls in the API modules it implements).
  "io.github.joacovela16" %% "xplat-io-sharding" % xplatVersion,
  // A journal for persistent actors: sqlite, postgres, mariadb or mongodb.
  "io.github.joacovela16" %% "xplat-persistence-postgres" % xplatVersion,
  // The libraries log through SLF4J; pick a backend.
  "ch.qos.logback" % "logback-classic" % "1.5.18"
)
```

If you only need local actors, depend on `xplat-io-actor` instead of `xplat-io-sharding`.

## Modules

| Artifact | Contents | Depends on |
| --- | --- | --- |
| **API** | | |
| `xplat-core-api` | `ActorSystem`, `ActorRef`, `Behavior`, `Behaviors`, `Context`, `Signal`, `Codec`, `TransportSecurity` | cats, log4cats |
| `xplat-core-persistence` | `Persistence`, `Journal`, `JournalBuilder`, `Effects` | core-api, fs2 |
| `xplat-core-coordination` | `Lease`, `LeaseProvider`, `LeaseSettings` | |
| `xplat-core-cluster` | `Cluster`, `Receptionist`, `ClusterSingleton`, `ClusterConfig`, `SplitBrainResolverConfig` | core-api, core-coordination |
| `xplat-core-sharding` | `ClusterSharding`, `Entity`, `EntityRef`, `ShardingConfig`, `ReliableProducer`, `Deduplicator` | core-cluster |
| **Implementation (cats-effect)** | | |
| `xplat-io-actor` | `IOActorSystem`, `IOPersistence` | core-api, core-persistence |
| `xplat-io-coordination` | `StoreLeaseProvider`, `InMemoryLeaseStore`, `LeaseServer`, `RemoteLeaseStore` | core-coordination, fs2-io |
| `xplat-io-cluster` | `IOCluster`, `TcpTransport`, `InMemoryNetwork`, `IOClusterSingleton` | io-actor, io-coordination, core-cluster |
| `xplat-io-sharding` | `IOClusterSharding`, `IOReliableProducer`, `Deduplicators` | io-cluster, core-sharding |
| **Storage** | | |
| `xplat-persistence-jdbc` | `JdbcPersistence`: journal and lease store on any SQL database (doobie + HikariCP) | core-persistence, io-coordination |
| `xplat-persistence-sqlite` | `SqlitePersistence` | persistence-jdbc |
| `xplat-persistence-postgres` | `PostgresPersistence` | persistence-jdbc |
| `xplat-persistence-mariadb` | `MariaDbPersistence` | persistence-jdbc |
| `xplat-persistence-mongodb` | `MongoPersistence` | core-persistence, io-coordination |
| `xplat-persistence-testkit` | `JournalContract`, `LeaseStoreContract`: test suites for your own journal or lease store | io-actor, scalatest |

## Examples

All the examples below are in [`modules/examples`](modules/examples/src/main/scala/examples/ReadmeExamples.scala) and
compile with the build. Run one with `sbt "examples/runMain examples.ActorsExample"`.

### Actors

A behavior describes how an actor handles the next message. Returning a new behavior changes the state; `b.same` keeps it.

```scala
import cats.effect.{IO, IOApp}
import xplat.core.api._
import xplat.core.io.actor.IOActorSystemBuilder._

object ActorsExample extends IOApp.Simple {

  sealed trait Counter
  case object Increment extends Counter
  final case class Get(replyTo: Ref[Int]) extends Counter

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
```

`Behaviors` also gives access to the actor context, timers, stash and signals. A handler can return an `F[Unit]`, which
keeps the current behavior, or an `F[Behavior]`.

```scala
def worker: BehaviorBuilder[IO, Job] = b =>
  b.context { ctx =>
    // One-shot timer: sends Tick to this actor after 5 seconds.
    b.timer(5.seconds, Tick) { _ =>
      b.message {
        case Run(task) => ctx.log.info(s"running $task")
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
```

The context also spawns child actors (`ctx.spawn`), stops them (`ctx.stopChild`) and schedules effects (`ctx.schedule`).

### Persistent actors (event sourcing)

Commands decide which events to store; events update the state. When the actor starts again, it rebuilds its state from
the last snapshot plus the events after it.

```scala
import xplat.core.io.actor.IOPersistence._
import xplat.core.persistence.Persistence
import xplat.core.persistence.api.{Effects, JournalBuilder}
import xplat.extensions.persistence.sqlite.SqlitePersistence

sealed trait AccountCmd
final case class Deposit(amount: Long) extends AccountCmd
final case class Balance(replyTo: Ref[Long]) extends AccountCmd
case object TakeSnapshot extends AccountCmd

final case class Deposited(amount: Long)

def onCommand(balance: Long, cmd: AccountCmd): IO[Effects[Deposited]] = cmd match {
  case Deposit(amount)  => IO.pure(Effects[Deposited].persist(Deposited(amount)))
  case Balance(replyTo) => IO(replyTo ! balance).as(Effects[Deposited])
  case TakeSnapshot     => IO.pure(Effects[Deposited].compress) // snapshot of the state
}

def onEvent(balance: Long, event: Deposited): Long = balance + event.amount

// Plus implicit Codec[Long, Array[Byte]] and Codec[Deposited, Array[Byte]] for the state and the events.

SqlitePersistence.resource[IO](Paths.get("accounts.db")).use { db =>
  implicit val journals: JournalBuilder[IO] = db.journalBuilder
  val account = Persistence[IO, Long, AccountCmd, Deposited]("account", 0L, onCommand, onEvent)

  ActorSystem[IO].use { system =>
    for {
      // Events are stored under Shard("account", "acc-1"): restarting the actor replays them.
      ref     <- system.spawn[AccountCmd]("acc-1")(b => account(b))
      _       <- ref ! Deposit(100)
      balance <- ref.ask[Long](Balance(_))
    } yield balance
  }
}
```

Besides `persist` and `compress`, `Effects` has `stash` / `unstash` (hold commands until the entity is ready) and
`ignore`. By default a persistent actor stops after one minute without messages (`Persistence.Config(shutdownOnIdle)`).

State and events are stored as bytes through a `Codec[A, Array[Byte]]`, so you can use any serialization library:

```scala
implicit val longCodec: Codec[Long, Array[Byte]] = new Codec[Long, Array[Byte]] {
  def encode(from: Long): Either[CodecError, Array[Byte]] = Right(from.toString.getBytes(UTF_8))
  def decode(to: Array[Byte]): Either[CodecError, Long] = new String(to, UTF_8).toLongOption.toRight(CodecError("not a number"))
}
```

#### Storage backends

Each backend gives a `journalBuilder` (for persistent actors) and a `leaseStore` (for leases), sharing one connection pool.
Tables or collections are created on start.

```scala
SqlitePersistence.resource[IO](Paths.get("app.db"))
PostgresPersistence.resource[IO](JdbcConfig("jdbc:postgresql://db:5432/app", user = "app", password = "secret"))
MariaDbPersistence.resource[IO](JdbcConfig("jdbc:mariadb://db:3306/app", user = "app", password = "secret"))
MongoPersistence.resource[IO](MongoConfig("mongodb://db:27017", database = "app"))
```

To store events elsewhere, implement `Journal[F]` and check it with `JournalContract` from `xplat-persistence-testkit`.

### Cluster

Nodes join through seed nodes and keep a shared view of the members. The receptionist lets actors find each other on
any node.

```scala
import xplat.core.cluster.Cluster
import xplat.core.cluster.api._
import xplat.core.io.actor.IOActorSystem
import xplat.core.io.cluster.IOCluster
import xplat.core.io.cluster.transport.TcpTransport

implicit val clusterBuilder: ClusterBuilder[IO] = IOCluster.builder[IO](TcpTransport[IO])

val seeds = List(Address("shop", "10.0.0.1", 2552), Address("shop", "10.0.0.2", 2552))

def node(host: String): Resource[IO, Cluster[IO]] =
  for {
    system  <- IOActorSystem[IO]("shop") // every node uses the same system name
    cluster <- Cluster(system, ClusterConfig(host = host, port = 2552, seedNodes = seeds))
  } yield cluster

val LoggerKey = ServiceKey[String]("audit-logger") // needs an implicit Codec[String, Array[Byte]]

node("10.0.0.1").use { cluster =>
  for {
    _      <- cluster.awaitUp
    logger <- cluster.system.spawn[String]("audit")(b => b.message(line => IO.println(line)))
    _      <- cluster.receptionist.register(LoggerKey, logger)
    found  <- cluster.receptionist.find(LoggerKey)
    _      <- found.services.toList.traverse_(_ ! "hello")
    _ <- cluster.events
      .collect { case ClusterEvent.MemberUp(m) => m }
      .evalMap(m => IO.println(s"member up: ${m.address}"))
      .compile
      .drain
  } yield ()
}
```

Releasing the resource leaves the cluster gracefully. `ClusterConfig` also sets the node roles, the gossip and heartbeat
intervals, the failure detector and the split brain resolver.

#### Split brain resolver

When the network splits, the resolver downs one side so the same singleton or entity never runs twice:

```scala
ClusterConfig(
  splitBrainResolver = Some(SplitBrainResolverConfig(strategy = SplitBrainStrategy.KeepMajority()))
)
```

Strategies: `KeepMajority`, `StaticQuorum`, `KeepOldest`, `KeepReferee`, `DownAllNodes`, and `LeaseMajority`, which
asks an external lease (see [Leases](#leases-and-security)).

### Cluster singleton

```scala
import xplat.core.cluster.ClusterSingleton
import xplat.core.io.cluster.IOClusterSingleton

implicit val singletonBuilder: ClusterSingletonBuilder[IO] = IOClusterSingleton.builder[IO]

ClusterSingleton(cluster).use { singletons =>
  for {
    // Runs on the oldest node; moves to the next one when it leaves. Messages sent meanwhile are buffered.
    ref <- singletons.init(SingletonActor[IO, String]("scheduler")(b => b.message(job => IO.println(s"scheduling $job"))))
    _   <- ref ! "nightly-report"
  } yield ()
}
```

`SingletonConfig` restricts it to a role, sets the buffer size and can require a lease.

### Sharding

Entities are actors identified by a type and an id. Each one runs on exactly one node, and any node can send to it.

```scala
import xplat.core.io.sharding.IOClusterSharding
import xplat.core.sharding.ClusterSharding
import xplat.core.sharding.api._

sealed trait CartCmd
final case class AddItem(sku: String) extends CartCmd
final case class GetItems(replyTo: Ref[String]) extends CartCmd

val CartKey = EntityTypeKey[CartCmd]("cart")

def cart(b: Behaviors[IO, CartCmd], items: List[String]): Behavior[IO, CartCmd] =
  b.message {
    case AddItem(sku) => cart(b, sku :: items)
    case GetItems(replyTo) =>
      replyTo ! items.reverse.mkString(",")
      b.same
  }

implicit val shardingBuilder: ClusterShardingBuilder[IO] = IOClusterSharding.builder[IO]()

ClusterSharding(cluster).use { sharding =>
  val entity = Entity[IO, CartCmd](CartKey)(ctx => b => cart(b, Nil))(cartCodec(cluster.refResolver))
    .withConfig(ShardingConfig(role = Some("backend")))
  for {
    _     <- sharding.init(entity) // on every node that hosts or sends to carts
    cart   = sharding.entityRefFor(CartKey, "user-42")
    _     <- cart ! AddItem("book")
    items <- cart.ask[String](GetItems(_)) // needs an implicit Codec[String, Array[Byte]] for the reply
  } yield items
}
```

Messages to entities may travel to another node, so the entity needs a `Codec` for them. Reply refs travel as text
through the node's `RefResolver`:

```scala
def cartCodec(resolver: RefResolver[IO]): Codec[CartCmd, Array[Byte]] = new Codec[CartCmd, Array[Byte]] {
  def encode(from: CartCmd): Either[CodecError, Array[Byte]] = Right((from match {
    case AddItem(sku)      => s"add\n$sku"
    case GetItems(replyTo) => s"get\n${resolver.serialize(replyTo)}"
  }).getBytes(UTF_8))

  def decode(to: Array[Byte]): Either[CodecError, CartCmd] = new String(to, UTF_8).split("\n", 2) match {
    case Array("add", sku) => Right(AddItem(sku))
    case Array("get", ref) => Right(GetItems(resolver.resolve[String](ref)))
    case _                 => Left(CodecError("unknown cart command"))
  }
}
```

A persistent behavior can be used directly as an entity: `Entity(AccountKey)(_ => Persistence("account", ...))`.
`ShardingConfig` controls passivation of idle entities, remembered entities, hand-off and rebalancing, and leases per
shard. `IOClusterSharding.builder(journals = Some(db.journalBuilder))` keeps the shard allocations in the journal, so they
survive a full cluster restart.

### Reliable delivery

A `ReliableProducer` stores each message before `send` returns and delivers it until the entity confirms it, also
after the producer restarts. The entity drops duplicates with a `Deduplicator`.

```scala
import xplat.core.io.sharding.delivery.{DeliveryCodec, Deduplicators, IOReliableProducer}
import xplat.core.sharding.delivery._

final case class Notify(delivery: Delivery[String])
val InboxKey = EntityTypeKey[Notify]("inbox")

def inbox(resolver: RefResolver[IO]): Entity[IO, Notify] =
  Entity[IO, Notify](InboxKey)(ctx => b =>
    b.context(_ =>
      Deduplicators.inMemory[IO].map { dedup =>
        b.message { case Notify(d) => dedup.handle(d)(text => IO.println(s"${ctx.entityId}: $text")) }
      }
    )
  )(inboxCodec(resolver)) // built on DeliveryCodec[IO, String](resolver)

IOReliableProducer.resource[IO, String, Notify]("mailer", sharding, InboxKey, db.journalBuilder)(Notify(_)).use { producer =>
  producer.send("user-42", "your order shipped")
}
```

Use `Deduplicators.journaled` to remember processed messages across restarts, or keep a `DeliveryProgress` in the state
of a persistent entity.

### Leases and security

A lease is an exclusive permission with an expiry, kept outside the cluster. Singletons, shards and the `LeaseMajority`
split brain strategy use it so that work never runs on two nodes, even when the cluster itself cannot agree.

```scala
import xplat.core.io.coordination.{LeaseServer, RemoteLeaseStore, StoreLeaseProvider}

// Leases in the database that also stores the journal...
val fromDatabase: Resource[IO, LeaseProvider[IO]] =
  PostgresPersistence.resource[IO](JdbcConfig("jdbc:postgresql://db:5432/app", "app", "secret"))
    .map(db => StoreLeaseProvider[IO](db.leaseStore))

// ...or in a small lease server, reached over TLS.
val server = Resource.eval(InMemoryLeaseStore[IO]).flatMap(store => LeaseServer.bind[IO](host"0.0.0.0", port"7355", store, security))
val fromServer = RemoteLeaseStore[IO](SocketAddress(host"leases.internal", port"7355"), security).map(StoreLeaseProvider[IO](_))

def clusterBuilder(leases: LeaseProvider[IO]): ClusterBuilder[IO] = IOCluster.builder[IO](TcpTransport[IO], Some(leases))
```

Connections between nodes (and to the lease server) can use mutual TLS and a shared secret:

```scala
val security = TransportSecurity(
  tls = Some(TlsConfig(Paths.get("node.p12"), "changeit", Paths.get("truststore.p12"), "changeit")),
  sharedSecret = Some(sys.env("CLUSTER_SECRET"))
)

ClusterConfig(
  security = security,
  splitBrainResolver = Some(SplitBrainResolverConfig(SplitBrainStrategy.LeaseMajority("shop-sbr")))
)
```

### Testing

`InMemoryNetwork` runs several nodes in one JVM and can split and heal the network:

```scala
InMemoryNetwork[IO].flatMap { network =>
  val builder = IOCluster.builder[IO](network)
  // start nodes with Cluster(system, config)(builder), then network.partition(sideA, sideB) and network.heal
}
```

`StoreLeaseProvider.inMemory[IO]` gives leases without a database, and `xplat-persistence-testkit` has the contract
suites the storage backends are tested with.

## Building

```bash
sbt test            # the database tests start containers with podman or docker, and are skipped without them
sbt examples/run    # pick one of the examples
```

## License

[Apache License 2.0](LICENSE)
