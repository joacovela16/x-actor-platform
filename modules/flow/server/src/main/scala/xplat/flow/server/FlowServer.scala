package xplat.flow.server

import xplat.flow.nodes.ConnectionKind
import cats.effect.{Async, Deferred, Resource}
import cats.effect.std.Console
import cats.syntax.all._
import com.comcast.ip4s.{Host, Port}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import xplat.core.io.actor.IOActorSystem
import xplat.core.persistence.api.Journal
import xplat.extensions.persistence.mongodb.{MongoConfig, MongoPersistence}
import xplat.extensions.persistence.sqlite.SqlitePersistence
import xplat.flow.engine.{ActorFlowEngine, CorePlugin, CronScheduler, FlowMetrics, FlowObserver, FlowSchedules, FlowStore, FlowTracing, InMemoryJournal, SegmentMetrics}
import xplat.flow.entity.{EntityCatalog, EntityDef}
import xplat.flow.model.Types
import xplat.flow.ai.{AiPlugin, Cassettes}
import xplat.flow.db.DbPlugin
import xplat.flow.mongo.MongoPlugin
import xplat.flow.nodes.{ClientPool, Connections, EntityPlugin, FlowRunner, Traces}
import xplat.flow.http._
import xplat.flow.rest.HttpPlugin
import xplat.flow.queue.QueuePlugin
import xplat.flow.plugin.{FlowPlugin, PluginLoader, PluginStatus}
import xplat.flow.script.{ScriptNodes, ScriptPlugin}
import xplat.flow.stream.StreamPlugin

import scala.concurrent.duration._

/** The whole flow application, over any `F`: the actors, the journal, the engine, the stores, the clock that starts scheduled
  * flows, the consumers of the `queue` triggers and the HTTP server. The effect type is chosen by whoever starts it (see [[AppIO]]).
  */
object FlowServer {

  /** The plugins that come with the server: every kind of node it has. */
  private def builtins(config: ServerConfig): List[PluginLoader.Builtin] = {
    val cassettes = config.aiCassettes match {
      case ServerConfig.AiCassettes.Off         => Cassettes.Mode.Off
      case ServerConfig.AiCassettes.Record(dir) => Cassettes.Mode.Record(dir)
      case ServerConfig.AiCassettes.Replay(dir) => Cassettes.Mode.Replay(dir)
    }
    List(
      PluginLoader.Builtin(CorePlugin.Name, new CorePlugin),
      PluginLoader.Builtin(EntityPlugin.Name, new EntityPlugin),
      PluginLoader.Builtin(ScriptPlugin.Name, new ScriptPlugin(scala = config.scalaNodes)),
      PluginLoader.Builtin(DbPlugin.Name, new DbPlugin),
      PluginLoader.Builtin(MongoPlugin.Name, new MongoPlugin),
      PluginLoader.Builtin(HttpPlugin.Name, new HttpPlugin),
      PluginLoader.Builtin(QueuePlugin.Name, new QueuePlugin),
      PluginLoader.Builtin(StreamPlugin.Name, new StreamPlugin(extra = ScriptNodes.streamExtra)),
      PluginLoader.Builtin(AiPlugin.Name, new AiPlugin(cassettes))
    )
  }

  /** One line of the start log for a plugin. */
  private def describe(s: PluginStatus): String = {
    val who = s.manifest.fold(s.dir)(m => if (s.builtin) s"${m.name} (built-in)" else s"${m.name} ${m.version}")
    s.state match {
      case PluginStatus.State.Loaded(kinds, _, entities, triggers) =>
        s"plugin $who: ${(kinds ++ entities.map(e => s"entity $e") ++ triggers.map(t => s"$t triggers")).mkString(", ")}"
      case PluginStatus.State.Failed(reason)   => s"plugin $who NOT loaded: $reason"
    }
  }

  /** @param entities
    *   entities of the business model defined in code (with the DSL of `xplat.flow.entity.dsl`): they are offered like the ones
    *   edited in the UI, but cannot be changed there.
    */
  def resource[F[_]: Async: Console](config: ServerConfig, entities: List[EntityDef] = Nil): Resource[F, Server] = {
    val inFiles = config.database == ServerConfig.Database.Sqlite
    val inMongo = config.database == ServerConfig.Database.Mongo

    for {
      host <- Resource.eval(Async[F].fromOption(Host.fromString(config.host), new IllegalArgumentException(s"not a host: '${config.host}'")))
      port <- Resource.eval(Async[F].fromOption(Port.fromInt(config.port), new IllegalArgumentException(s"not a port: ${config.port}")))
      _ <- Resource.eval(Async[F].blocking(java.nio.file.Files.createDirectories(config.dataDir)).whenA(inFiles))
      system <- IOActorSystem[F]("flow")
      journal <- config.database match {
        case ServerConfig.Database.Sqlite => SqlitePersistence.resource[F](config.dataDir.resolve("flow.db")).map(_.journal)
        case ServerConfig.Database.Mongo  => MongoPersistence.resource[F](MongoConfig(config.mongo.uri, config.mongo.database, collectionPrefix = "flow_")).map(_.journal)
        case ServerConfig.Database.Memory => Resource.eval(InMemoryJournal.make[F].map(j => j: Journal[F]))
      }
      // The definitions, triggers and connections go where the instances go: files of the data directory, or MongoDB.
      state <- if (inMongo) MongoState.resource[F](config.mongo.uri, config.mongo.database, "flow_state").map(Some(_)) else Resource.pure[F, Option[MongoState[F]]](None)
      slot = (name: String, restricted: Boolean) =>
        state.map(_.slot(name)).orElse(Option.when(inFiles)(JsonSlot.file[F](config.dataDir.resolve(s"$name.json"), restricted)))
      secrets <- Resource.eval(SecretStore.inSlot[F](slot("secrets", true)))
      // The clients of connections (MongoDB, JDBC pools, publishers) are opened once and lent to the nodes. When a connection is
      // edited or deleted, its clients are closed as soon as they are returned.
      clients <- ClientPool.make[F](config.pool)
      // Every kind of node comes from a plugin: those of the server first, then those of the plugins directory. Plugins bring kinds
      // of connection, which the store needs, and their nodes find connections in that store, start flows with the engine and read
      // the entities: they get views of them that work once those exist (nodes only use them when they run).
      namedLater <- Resource.eval(Deferred[F, Connections[F]])
      runnerLater <- Resource.eval(Deferred[F, FlowRunner[F]])
      entitiesLater <- Resource.eval(Deferred[F, F[EntityCatalog]])
      // The nodes of streams report how they go to this, and the engine shows it in the status of the instances.
      metrics <- Resource.eval(SegmentMetrics.inMemory[F])
      // Traces, when there is a collector to send them to: made from the audit of the instances, sent by OTLP.
      tracing <- config.otel.traverse(o => OtlpTraces.resource[F](o).evalMap(FlowTracing.make[F](_)))
      _ <- Resource.eval(config.otel.traverse_(o => Console[F].println(s"traces go to ${o.endpoint} as '${o.service}'")))
      plugins <- PluginLoader.load[F](
        PluginLoader.Settings(config.pluginsDirectory, config.pluginsStrict),
        FlowPlugin.Context[F](
          (name: String) => namedLater.get.flatMap(_.resolve(name)),
          clients,
          FlowRunner.later(runnerLater.get),
          metrics,
          entitiesLater.get.flatten,
          tracing.fold(Traces.none[F])(t => t)
        ),
        ConnectionKind.defaults.map(_.kind).toSet,
        line => Console[F].errorln(line),
        builtins(config)
      )
      _ <- Resource.eval(plugins.statuses.traverse_(s => Console[F].println(describe(s))))
      _ <- Resource.eval(Console[F].println("script.scala is on: its code runs in the server without a sandbox").whenA(config.scalaNodes))
      connections <- Resource.eval(
        ConnectionStore.inSlot[F](
          ConnectionKind.defaults ++ plugins.connectionKinds,
          secrets,
          slot("connections", false),
          onChange = Some(clients.retire)
        )
      )
      // The entities of the business model: those edited in the UI, those of code and those of the plugins.
      entityStore <- Resource.eval(
        EntityStore.inSlot[F](
          slot("entities", false),
          entities.map(EntityStore.Fixed(_, EntityStore.Source.Code)) ++
            plugins.entities.map { case (plugin, e) => EntityStore.Fixed(e, EntityStore.Source.plugin(plugin)) }
        )
      )
      _ <- Resource.eval(entitiesLater.complete(entityStore.catalog))
      _ <- Resource.eval(entityStore.shadowed.flatMap(_.traverse_(n => Console[F].println(s"entity $n: the one defined in code is used, not the one edited"))))
      types = entityStore.catalog.widen[Types]
      triggers <- Resource.eval(TriggerRegistry.inSlot[F](slot("triggers", false)))
      deployments <- Resource.eval(JsonDeployments.inSlot[F](slot("deployments", false)))
      checkpoints <- Resource.eval(JsonCheckpoints.inSlot[F](slot("checkpoints", false)))
      drafts <- Resource.eval(FlowDrafts.inSlot[F](slot("drafts", false)))
      // The nodes that talk to brokers find their connections by name, with their secrets.
      named = ConnectionStore.provider[F](connections, secrets)
      _ <- Resource.eval(namedLater.complete(named))
      // What the instances do, measured from their audit: `/metrics` and the health view.
      flowMetrics <- Resource.eval(FlowMetrics.make[F]())
      engine <- Resource.eval(
        ActorFlowEngine.make[F](
          system,
          journal,
          extraNodes = plugins.nodes,
          config = ActorFlowEngine.Config(nodeTimeout = config.nodeTimeout),
          store = Some(catalog => JsonFlowStore.inSlot[F](catalog, slot("flows", false), types).map(s => s: FlowStore[F])),
          metrics = Some(metrics),
          types = Some(types),
          coreNodes = false,
          observer = Some(FlowObserver.all[F](flowMetrics :: tracing.toList))
        )
      )
      _ <- Resource.eval(runnerLater.complete(engine.runner))
      _ <- Resource.eval(Examples.install[F](engine.store, triggers, connections).whenA(config.examples))
      // Flows that are enabled and have a cron launcher, and the cron triggers, start instances by the clock.
      _ <- CronScheduler.resource[F](
        engine,
        (FlowSchedules.enabled(engine.store, deployments), TriggerSchedules.provider(triggers)).mapN(_ ++ _),
        checkpoints
      )
      // The triggers that plugins serve: every `queue` trigger reads its queue (a broker's, with `queue`; a collection's changes, with
      // `mongo`) and starts an instance for each message.
      triggerSources <- TriggerSources.resource[F](engine, triggers, named, plugins.triggers, tracing = tracing)
      routes = FlowHttp.routes[F](
        engine,
        triggers,
        connections,
        secrets,
        deployments,
        drafts,
        Some(clients),
        io.circe.Encoder[List[PluginStatus]].apply(plugins.statuses),
        entities = Some(entityStore),
        metrics = Some(MetricsRoutes.Sources(flowMetrics, triggerSources, Some(clients))),
        tracing = tracing
      )
      server <- EmberServerBuilder
        .default[F]
        .withHost(host)
        .withPort(port)
        .withHttpApp(routes.orNotFound)
        // Streams of events that are open should not keep a stop waiting.
        .withShutdownTimeout(3.seconds)
        .build
    } yield server
  }
}
