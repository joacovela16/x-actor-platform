package xplat.flow.plugin

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Decoder, Json}
import xplat.flow.entity.EntityDef
import xplat.flow.model._
import xplat.flow.nodes.{ConnectionKind, Connections, NodeType}

import java.nio.file.Path

/** A plugin in any language: a process started with the manifest's `command`, in the plugin's directory, that speaks JSON-RPC 2.0
  * over stdio, one message per line (see [[RpcProcess]]). The process lives as long as the server and is started again if it ends.
  *
  *  - `initialize` `{apiVersion, plugin: {name, version}}` → `{nodes: [descriptor], connectionKinds: [{kind, label, config}],
  *    entities: [entity]}` (`entities` are entities of the business model, as JSON; optional).
  *    A descriptor is data: `kind`, `label`, `category`, `config` (a [[ConfigSchema]]), `sideEffects`, `ports`, `inputs` and
  *    `outputs` (the [[OutputDecl]]s of a new node).
  *  - `run` `{kind, input, connection}` → a [[NodeOutput]]. `input` is the [[NodeInput]] with its params resolved; `connection`,
  *    when the params name one in `connection`, is it with its secrets (`{name, kind, params, secrets}`), only if it is of
  *    one of the plugin's own kinds.
  *  - `$/cancel` `{id}` (no answer): the server no longer waits for that request.
  *  - `shutdown` (no answer): the process should exit.
  *
  * In a simulation the nodes are not run: the engine answers with default values.
  */
object ProcessPlugin {

  def load[F[_]](dir: Path, m: PluginManifest, connections: Connections[F], log: String => F[Unit])(implicit
      F: Async[F]
  ): Resource[F, (List[NodeType[F]], List[ConnectionKind], List[EntityDef])] = {
    val env = Map("FLOW_PLUGIN_NAME" -> m.name, "FLOW_PLUGIN_API" -> FlowPlugin.ApiVersion)
    val init = (rpc: RpcProcess[F]) =>
      rpc
        .call("initialize", Json.obj("apiVersion" -> FlowPlugin.ApiVersion.asJson, "plugin" -> Json.obj("name" -> m.name.asJson, "version" -> m.version.toString.asJson)))
        .flatMap(r => F.fromEither(r.leftMap(e => PluginLoader.Problem(s"initialize failed: $e"))))
    for {
      started <- RpcProcess.Supervised(RpcProcess.start[F](m.command, dir, env, log))(init)
      (process, answer) = started
      declared <- Resource.eval(F.fromEither(answer.as[Declared].leftMap(e => PluginLoader.Problem(s"initialize answered something else: ${e.getMessage}"))))
    } yield (declared.nodes.map(d => node(m, d, process, connections)), declared.connectionKinds, declared.entities)
  }

  private final case class Declared(nodes: List[NodeDescriptor], connectionKinds: List[ConnectionKind], entities: List[EntityDef])

  private implicit val connectionKindDecoder: Decoder[ConnectionKind] =
    Decoder.forProduct3("kind", "label", "config")(ConnectionKind.apply)

  /** A descriptor sent as data: everything that depends on the params is fixed. */
  private implicit val descriptorDecoder: Decoder[NodeDescriptor] = Decoder.instance { c =>
    for {
      kind <- c.get[String]("kind")
      label <- c.getOrElse[String]("label")(kind)
      category <- c.getOrElse[String]("category")("Plugins")
      config <- c.getOrElse[ConfigSchema]("config")(ConfigSchema.empty)
      effects <- c.getOrElse[SideEffects]("sideEffects")(SideEffects.Pure)
      ports <- c.getOrElse[List[Port]]("ports")(List(Port(Port.Out)))
      inputs <- c.getOrElse[List[Port]]("inputs")(List(Port(Port.In)))
      outputs <- c.getOrElse[List[OutputDecl]]("outputs")(Nil)
      description <- c.getOrElse[String]("description")("")
    } yield NodeDescriptor(kind, category, label, config, effects, ports = _ => ports, defaultOutputs = _ => outputs, inputs = _ => inputs, description = description)
  }

  private implicit val declaredDecoder: Decoder[Declared] = Decoder.instance { c =>
    (
      c.getOrElse[List[NodeDescriptor]]("nodes")(Nil),
      c.getOrElse[List[ConnectionKind]]("connectionKinds")(Nil),
      c.getOrElse[List[EntityDef]]("entities")(Nil)
    ).mapN(Declared.apply)
  }

  private def own(m: PluginManifest, kind: String): Boolean = kind == m.name || kind.startsWith(s"${m.name}.")

  private def node[F[_]](m: PluginManifest, d: NodeDescriptor, process: RpcProcess.Supervised[F], connections: Connections[F])(implicit F: Async[F]): NodeType[F] =
    new NodeType[F] {
      val descriptor: NodeDescriptor = d

      /** The connection the params name, if it is one of the plugin's. */
      private def connection(params: Json): F[Either[String, Json]] =
        params.hcursor.get[String]("connection").toOption.filter(_.nonEmpty) match {
          case None => F.pure(Right(Json.Null))
          case Some(name) =>
            connections.resolve(name).map {
              case None                       => Left(s"the connection '$name' does not exist")
              case Some(c) if !own(m, c.kind) => Left(s"the connection '$name' is of kind '${c.kind}', which is not one of the plugin ${m.name}'s")
              case Some(c) =>
                Right(Json.obj("name" -> c.name.asJson, "kind" -> c.kind.asJson, "params" -> c.params, "secrets" -> c.secrets.asJson))
            }
        }

      def run(input: NodeInput): F[NodeOutput] =
        connection(input.params).flatMap {
          case Left(e) => F.pure(NodeOutput.failed(e))
          case Right(conn) =>
            process.call("run", Json.obj("kind" -> d.kind.asJson, "input" -> input.asJson, "connection" -> conn)).map {
              case Left(e)  => NodeOutput.failed(e)
              case Right(j) => j.as[NodeOutput].fold(e => NodeOutput.failed(s"the plugin answered something that is not an output: ${e.getMessage}"), identity)
            }
        }
    }
}
