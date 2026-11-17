package xplat.flow.plugin

import cats.Applicative
import cats.effect.kernel.{Async, Resource}
import xplat.flow.engine.SegmentMetrics
import xplat.flow.entity.{EntityCatalog, EntityDef}
import xplat.flow.nodes.{ClientPool, ConnectionKind, Connections, FlowRunner, NodeType, Traces, TriggerSource}

/** What a plugin brings to the server: kinds of node, kinds of connection and entities of the business model. Every node of the
  * server comes from a plugin: the built-in ones (`core`, `db`, `stream`...) implement this same trait.
  *
  * An external plugin is a jar with a `flow-plugin.json` manifest (see [[PluginManifest]]) and a `ServiceLoader` entry
  * `META-INF/services/xplat.flow.plugin.FlowPlugin` naming its implementation, which needs a constructor without
  * arguments. It is compiled against `flow-nodes-api` as `Provided`: the server brings the API, cats-effect and circe.
  *
  * Kinds of node of an external plugin are named without prefix (`lookup`); the server registers them as
  * `<plugin>.<kind>@<major>` (`ipa.lookup@1`), so plugins never collide and two majors of one plugin can be installed side by
  * side. Built-in plugins name their kinds in full (`db.query`). Kinds of connection keep their name, which must be the
  * plugin's name or start with it (`ipa`, `ipa.oauth`).
  *
  * Services of the plugin (pools, clients, caches, logic its nodes share) are built in [[install]] and seen by its nodes only:
  * no other plugin can reach them. Services of the server come in the [[FlowPlugin.Context]].
  */
trait FlowPlugin {

  /** What the plugin installs, built once when the server starts and released when it stops (a client opened here is closed
    * then). Overrides [[nodes]]; a plugin written for API 1.0 implements `nodes` only.
    */
  def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] =
    nodes[F](context).map(FlowPlugin.Installed(_))

  /** The nodes, for a plugin that only brings nodes. Prefer [[install]]. */
  def nodes[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, List[NodeType[F]]] = Resource.pure(Nil)

  def connectionKinds: List[ConnectionKind] = Nil

  /** Entities of the business model, written with the DSL of `xplat.flow.entity.dsl`. They keep their names (no prefix) and cannot
    * be edited in the server; two plugins that bring an entity with the same name cannot both be loaded.
    */
  def entities: List[EntityDef] = Nil
}

object FlowPlugin {

  /** The version of this API. A server loads the plugins built for its major version. */
  val ApiVersion: String = "1.2.0"

  /** What a plugin installed: kinds of node, and sources that start flows for triggers. */
  final case class Installed[F[_]](nodes: List[NodeType[F]], triggers: List[TriggerSource[F]] = Nil)

  /** What the server lends to the nodes of a plugin.
    *
    * @param connections
    *   the named connections, with their secrets resolved.
    * @param clients
    *   the pool of clients of the server, for clients that are expensive to open (see [[ClientPool]]).
    * @param runner
    *   starts other flows (and waits for them, if asked). It works once the engine is up: nodes only use it when they run.
    * @param metrics
    *   where nodes that run for long (stream segments) say how they go; the status of the instances shows it.
    * @param entities
    *   the entities of the business model as they are each time they are asked for.
    * @param traces
    *   the `traceparent` of a running node, for the nodes that call other systems to send it on.
    */
  final case class Context[F[_]](
      connections: Connections[F],
      clients: ClientPool[F],
      runner: FlowRunner[F],
      metrics: SegmentMetrics[F],
      entities: F[EntityCatalog],
      traces: Traces[F]
  )

  object Context {

    /** A context with only connections and clients: no flows to run, metrics that go nowhere, no entities. For tests and tools. */
    def basic[F[_]: Applicative](connections: Connections[F], clients: ClientPool[F]): Context[F] =
      Context(connections, clients, FlowRunner.none[F], SegmentMetrics.none[F], Applicative[F].pure(EntityCatalog.empty), Traces.none[F])
  }
}
