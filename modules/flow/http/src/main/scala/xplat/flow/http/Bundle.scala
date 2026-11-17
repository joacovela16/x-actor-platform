package xplat.flow.http

import cats.effect.Async
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json, Printer}
import xplat.flow.engine.{Deployments, FlowEngine}
import xplat.flow.entity.{EntityCatalog, EntityDef, EntityValidator}
import xplat.flow.model.{FlowDef, FlowResolver, FlowValidator}

/** What moves between servers (dev → staging → prod): flows with whether they are enabled, the triggers that start them, the
  * entities edited in the server and the connections they name, without revisions and without secrets, so the same content
  * always gives the same text and it can be kept in git.
  */
final case class Bundle(
    flows: List[Bundle.Flow],
    triggers: List[TriggerDef] = Nil,
    entities: List[EntityDef] = Nil,
    connections: List[Bundle.Connection] = Nil,
    requires: Bundle.Requires = Bundle.Requires()
)

object Bundle {

  val Format: String = "x-flow-bundle"
  val Version: Int = 1

  final case class Flow(flow: FlowDef, enabled: Boolean)
  final case class Connection(name: String, kind: String, params: Json)

  /** What the target needs to have: kinds of node and of connection. */
  final case class Requires(kinds: List[String] = Nil, connectionKinds: List[String] = Nil)

  /** What an import does, or would do, with one item. */
  final case class Item(`type`: String, id: String, action: String)

  /** @param problems what stops the import; `warnings`, what does not. */
  final case class Plan(items: List[Item], problems: List[String], warnings: List[String]) {
    def ok: Boolean = problems.isEmpty
  }

  object Action {
    val Create = "create"
    val Update = "update"
    val Same = "same"
    val Skip = "skip"

    /** A flow that needs secrets the target does not have: it goes in as a draft, to complete and publish there. */
    val Draft = "draft"
  }

  /** The text of a bundle: keys sorted and two spaces, so the same content always gives the same text. */
  val printer: Printer = Printer.spaces2SortKeys.copy(dropNullValues = true)

  // ---- JSON ----

  private def withoutRevision(json: Json): Json = json.mapObject(_.remove("revision"))

  implicit val flowEncoder: Encoder[Flow] = Encoder.instance(f => Json.obj("flow" -> withoutRevision(f.flow.asJson), "enabled" -> f.enabled.asJson))
  implicit val flowDecoder: Decoder[Flow] = Decoder.instance(c => (c.get[FlowDef]("flow"), c.getOrElse[Boolean]("enabled")(false)).mapN(Flow.apply))
  implicit val connectionEncoder: Encoder[Connection] = Encoder.forProduct3("name", "kind", "params")(c => (c.name, c.kind, c.params))
  implicit val connectionDecoder: Decoder[Connection] =
    Decoder.instance(c => (c.get[String]("name"), c.get[String]("kind"), c.getOrElse[Json]("params")(Json.obj())).mapN(Connection.apply))
  implicit val requiresEncoder: Encoder[Requires] = Encoder.forProduct2("kinds", "connectionKinds")(r => (r.kinds, r.connectionKinds))
  implicit val requiresDecoder: Decoder[Requires] =
    Decoder.instance(c => (c.getOrElse[List[String]]("kinds")(Nil), c.getOrElse[List[String]]("connectionKinds")(Nil)).mapN(Requires.apply))
  implicit val itemEncoder: Encoder[Item] = Encoder.forProduct3("type", "id", "action")(i => (i.`type`, i.id, i.action))
  implicit val planEncoder: Encoder[Plan] = Encoder.forProduct3("items", "problems", "warnings")(p => (p.items, p.problems, p.warnings))

  implicit val encoder: Encoder[Bundle] = Encoder.instance { b =>
    Json.obj(
      "format" -> Format.asJson,
      "version" -> Version.asJson,
      "flows" -> b.flows.asJson,
      "triggers" -> b.triggers.asJson,
      "entities" -> Json.fromValues(b.entities.map(e => withoutRevision(e.asJson))),
      "connections" -> b.connections.asJson,
      "requires" -> b.requires.asJson
    )
  }

  implicit val decoder: Decoder[Bundle] = Decoder.instance { c =>
    for {
      format <- c.get[String]("format")
      _ <- Either.raiseWhen(format != Format)(DecodingFailure(s"not a bundle: format '$format', expected '$Format'", c.history))
      version <- c.get[Int]("version")
      _ <- Either.raiseWhen(version > Version)(DecodingFailure(s"bundle version $version; this server reads up to $Version", c.history))
      flows <- c.getOrElse[List[Flow]]("flows")(Nil)
      triggers <- c.getOrElse[List[TriggerDef]]("triggers")(Nil)
      entities <- c.getOrElse[List[EntityDef]]("entities")(Nil)
      connections <- c.getOrElse[List[Connection]]("connections")(Nil)
      requires <- c.getOrElse[Requires]("requires")(Requires())
    } yield Bundle(flows, triggers, entities, connections, requires)
  }

  // ---- the server's side ----

  /** The connection a node names, if it names one. */
  private def connectionOf(params: Json): Option[String] = params.hcursor.get[String]("connection").toOption.filter(_.nonEmpty)

  /** Exports and imports bundles against the stores of a server. */
  final class Service[F[_]](
      engine: FlowEngine[F],
      triggers: TriggerRegistry[F],
      connections: ConnectionStore[F],
      deployments: Deployments[F],
      drafts: FlowDrafts[F],
      entities: Option[EntityStore[F]]
  )(implicit F: Async[F]) {

    private val catalog = engine.catalog

    /** The secret params (`node.field`) that `flow` still lacks. */
    private def missingSecrets(flow: FlowDef): List[String] =
      flow.nodes.flatMap(n => FlowSecrets.secretFields(catalog, n).toList.sorted.filterNot(f => n.params.asObject.exists(_.contains(f))).map(f => s"${n.id}.$f"))

    /** The flow with a stand-in for each secret it lacks, to validate the rest of it. */
    private def withStandIns(flow: FlowDef): FlowDef =
      flow.copy(nodes = flow.nodes.map { n =>
        val lacking = FlowSecrets.secretFields(catalog, n).filterNot(f => n.params.asObject.exists(_.contains(f)))
        if (lacking.isEmpty) n else n.copy(params = n.params.deepMerge(Json.fromFields(lacking.map(_ -> Json.fromString("secret")))))
      })

    /** The flow as it would be saved here: the secrets of the one here kept. */
    private def target(f: Flow, existing: Option[FlowDef]): FlowDef = FlowSecrets.keep(f.flow, existing, catalog)

    private def unchanged(f: Flow, existing: Option[FlowDef]): Boolean =
      existing.exists(old => FlowSecrets.strip(old, catalog).copy(revision = 1) == f.flow.copy(revision = 1))

    /** The flows `ids` (every flow if empty) with what they need. An id that is not a flow is a problem. */
    def export(ids: List[String]): F[Either[List[String], Bundle]] =
      for {
        summaries <- engine.store.list
        wanted = if (ids.isEmpty) summaries.map(_.id) else ids.distinct
        found <- wanted.traverse(id => engine.store.get(id, None).map(id -> _))
        missing = found.collect { case (id, None) => s"flow '$id' does not exist" }
        flows = found.collect { case (_, Some(f)) => FlowSecrets.strip(f, catalog) }
        enabled <- deployments.enabled
        allTriggers <- triggers.list
        theirs = allTriggers.filter(t => flows.exists(_.id == t.flowId))
        named = (flows.flatMap(_.nodes.flatMap(n => connectionOf(n.params))) ++ theirs.flatMap(_.connection)).distinct.sorted
        views <- named.traverse(connections.get).map(_.flatten)
        edited <- entities.fold(F.pure(List.empty[EntityDef]))(_.all.map(_.collect { case (e, EntityStore.Source.Store) => e }))
      } yield
        if (missing.nonEmpty) Left(missing)
        else
          Right(
            Bundle(
              flows.sortBy(_.id).map(f => Flow(f, enabled.contains(f.id))),
              theirs.sortBy(_.id),
              edited.sortBy(_.name),
              views.map(v => Connection(v.name, v.kind, v.params)),
              Requires(
                flows.flatMap(_.nodes.map(_.kind)).distinct.sorted,
                views.map(_.kind).distinct.sorted
              )
            )
          )

    /** What importing `bundle` would do, without doing it. */
    def plan(bundle: Bundle): F[Plan] =
      for {
        currentEntities <- entities.fold(F.pure(EntityCatalog.empty))(_.catalog)
        entitySources <- entities.fold(F.pure(Map.empty[String, String]))(_.all.map(_.map { case (e, s) => e.name -> s }.toMap))
        baseResolver <- engine.store.resolver
        existingFlows <- bundle.flows.traverse(f => engine.store.get(f.flow.id, None))
        existingTriggers <- bundle.triggers.traverse(t => triggers.get(t.id))
        existingConnections <- bundle.connections.traverse(c => connections.get(c.name))
        enabled <- deployments.enabled
      } yield {
        val types = bundle.entities.foldLeft(currentEntities)(_.updated(_))
        val resolver = new FlowResolver {
          def find(flowId: String, revision: Option[Int]): Option[FlowDef] =
            bundle.flows.map(_.flow).find(f => f.id == flowId && revision.isEmpty).orElse(baseResolver.find(flowId, revision))
        }

        val unknownKinds = bundle.requires.kinds.filterNot(k => catalog.get(k).isDefined) ++
          bundle.flows.flatMap(_.flow.nodes.map(_.kind)).filterNot(k => catalog.get(k).isDefined)
        val connectionKinds = connections.kinds.map(k => k.kind -> k).toMap
        val unknownConnectionKinds = (bundle.requires.connectionKinds ++ bundle.connections.map(_.kind)).filterNot(connectionKinds.contains)

        val entityItems = bundle.entities.map { e =>
          val problems = entitySources.get(e.name).filter(_ != EntityStore.Source.Store).map(s => s"entity '${e.name}' is defined by $s here") ++
            EntityValidator.validate(e, types).map(err => s"entity '${e.name}': ${err.message}")
          val action = currentEntities.get(e.name) match {
            case None                                                  => Action.Create
            case Some(old) if old.copy(revision = 1) == e.copy(revision = 1) => Action.Same
            case Some(_)                                               => Action.Update
          }
          (Item("entity", e.name, action), problems.toList, Nil)
        }

        val connectionItems = bundle.connections.zip(existingConnections).map {
          case (c, Some(v)) =>
            val differs = v.kind != c.kind || v.params != c.params
            (Item("connection", c.name, Action.Skip), Nil, if (differs) List(s"connection '${c.name}' exists here with other settings: it is left as it is") else Nil)
          case (c, None) =>
            val secrets = connectionKinds.get(c.kind).toList.flatMap(_.config.fields.filter(f => f.secret && f.required && f.default.isEmpty).map(_.name))
            if (secrets.isEmpty) (Item("connection", c.name, Action.Create), Nil, Nil)
            else
              (
                Item("connection", c.name, Action.Skip),
                Nil,
                List(s"connection '${c.name}' (${c.kind}) does not exist here and needs secrets (${secrets.mkString(", ")}): create it before running its flows")
              )
        }

        val flowItems = bundle.flows.zip(existingFlows).map { case (f, existing) =>
          val saved = target(f, existing)
          val lacking = missingSecrets(saved)
          val errors = FlowValidator.validate(withStandIns(saved), catalog, resolver, types).map(e => s"flow '${f.flow.id}'${e.node.fold("")(n => s", node '$n'")}: ${e.message}")
          val action =
            if (unchanged(f, existing)) { if (enabled.contains(f.flow.id) == f.enabled) Action.Same else Action.Update }
            else if (lacking.nonEmpty) Action.Draft
            else if (existing.isEmpty) Action.Create
            else Action.Update
          val warnings = Option.when(action == Action.Draft)(
            s"flow '${f.flow.id}' lacks secret params here (${lacking.mkString(", ")}): it goes in as a draft, to complete and publish"
          ).toList
          (Item("flow", f.flow.id, action), errors, warnings)
        }

        val triggerItems = bundle.triggers.zip(existingTriggers).map { case (t, existing) =>
          val action = existing match {
            case None                => Action.Create
            case Some(old) if old == t => Action.Same
            case Some(_)             => Action.Update
          }
          val noFlow = Option.when(!bundle.flows.exists(_.flow.id == t.flowId) && baseResolver.find(t.flowId, None).isEmpty)(
            s"trigger '${t.id}' starts '${t.flowId}', which is neither in the bundle nor here"
          )
          (Item("trigger", t.id, action), t.problems.map(p => s"trigger '${t.id}': $p") ++ noFlow, Nil)
        }

        val all = entityItems ++ connectionItems ++ flowItems ++ triggerItems
        Plan(
          all.map(_._1),
          unknownKinds.distinct.sorted.map(k => s"this server has no node kind '$k'") ++
            unknownConnectionKinds.distinct.sorted.map(k => s"this server has no connection kind '$k'") ++ all.flatMap(_._2),
          all.flatMap(_._3)
        )
      }

    /** Imports `bundle` if its plan has no problems: entities, connections, flows, triggers, and then which flows are enabled. */
    def importing(bundle: Bundle): F[Either[Plan, Plan]] =
      plan(bundle).flatMap { p =>
        if (!p.ok) F.pure(Left(p))
        else {
          def action(kind: String, id: String): String = p.items.find(i => i.`type` == kind && i.id == id).fold(Action.Skip)(_.action)
          def doing(kind: String, id: String): Boolean = Set(Action.Create, Action.Update).contains(action(kind, id))
          def fail(what: String)(errors: List[String]): F[Unit] = F.raiseError(new IllegalStateException(s"$what: ${errors.mkString("; ")}"))
          for {
            _ <- entities.traverse_(store =>
              bundle.entities.filter(e => doing("entity", e.name)).traverse_(e =>
                store.save(e).flatMap(_.fold(r => fail(s"entity '${e.name}'")(List(r.toString)), _ => F.unit))
              )
            )
            _ <- bundle.connections.filter(c => doing("connection", c.name)).traverse_(c =>
              connections.put(c.name, c.kind, c.params, Map.empty).flatMap(_.fold(fail(s"connection '${c.name}'"), _ => F.unit))
            )
            _ <- bundle.flows.filter(f => doing("flow", f.flow.id) || action("flow", f.flow.id) == Action.Draft).traverse_ { f =>
              engine.store.get(f.flow.id, None).flatMap { previous =>
                if (action("flow", f.flow.id) == Action.Draft) drafts.put(target(f, previous))
                else if (unchanged(f, previous)) F.unit
                else engine.store.save(target(f, previous)).flatMap(_.fold(e => fail(s"flow '${f.flow.id}'")(e.map(_.message)), _ => F.unit))
              }
            }
            _ <- bundle.triggers.filter(t => doing("trigger", t.id)).traverse_(t => triggers.put(t).flatMap(_.fold(fail(s"trigger '${t.id}'"), _ => F.unit)))
            // A flow left as a draft is not enabled: there is nothing published to run.
            _ <- bundle.flows.filter(f => doing("flow", f.flow.id)).traverse_(f => deployments.set(f.flow.id, f.enabled))
          } yield Right(p)
        }
      }
  }
}
