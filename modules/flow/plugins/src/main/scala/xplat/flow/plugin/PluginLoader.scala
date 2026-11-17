package xplat.flow.plugin

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import io.circe.parser.decode
import xplat.flow.entity.EntityDef
import xplat.flow.model.{DataType, NodeDescriptor, NodeInput, NodeOutput, StateRead}
import xplat.flow.nodes.{ConnectionKind, NodeType, Template, TriggerSource}
import xplat.flow.plugin.PluginManifest.Transport
import xplat.flow.plugin.PluginStatus.State

import java.nio.file.{Files, Path}
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._

/** Loads the built-in plugins of the server and then finds the plugins of a directory, one per subdirectory with a
  * [[PluginManifest]], and loads them.
  *
  *  - built-in: given by the server ([[Builtin]]). Their kinds keep the names they give (which must start with the plugin's
  *    name) and they resolve their own references; one that cannot be installed stops the start.
  *  - `jvm`: the jars of the directory (and of its `lib/`) go into a [[PluginClassLoader]] of their own; the
  *    [[FlowPlugin]]s that its `ServiceLoader` entries name build the nodes.
  *  - `process`: the manifest's `command` is started in the directory and speaks JSON-RPC over stdio (see [[ProcessPlugin]]).
  *
  * A plugin that cannot be loaded is reported and left out, unless `strict`, which stops the start. Two directories with the
  * same major of a plugin, a kind of connection that belongs to someone else or a kind of node named twice make it fail too.
  */
object PluginLoader {

  final case class Settings(dir: Path, strict: Boolean = false)

  /** A plugin that comes with the server, under `name`. Its settings are given to it when it is made. */
  final case class Builtin(name: String, plugin: FlowPlugin)

  /** @param entities
    *   the entities of the business model the plugins bring, each with the id of its plugin.
    */
  final case class Loaded[F[_]](
      nodes: List[NodeType[F]],
      connectionKinds: List[ConnectionKind],
      statuses: List[PluginStatus],
      entities: List[(String, EntityDef)] = Nil,
      triggers: List[TriggerSource[F]] = Nil
  ) {
    def failed: List[PluginStatus] = statuses.filter(_.state.isInstanceOf[State.Failed])
  }

  private val KindRe = "[a-z][a-zA-Z0-9_-]*".r

  private val ApiMajor: Int = PluginManifest.Version.parse(FlowPlugin.ApiVersion).fold(e => sys.error(e), _.major)

  /** @param builtInConnectionKinds
    *   the kinds of connection of the server, which a plugin cannot declare again.
    * @param log
    *   gets what the processes of `process` plugins write to stderr, each line with the plugin's id in front.
    * @param builtins
    *   the plugins of the server, installed first and in this order.
    */
  def load[F[_]](
      settings: Settings,
      context: FlowPlugin.Context[F],
      builtInConnectionKinds: Set[String],
      log: String => F[Unit],
      builtins: List[Builtin] = Nil
  )(implicit F: Async[F]): Resource[F, Loaded[F]] = {
    val reserved = builtInConnectionKinds ++ builtins.flatMap(_.plugin.connectionKinds.map(_.kind))
    val start = Acc[F](Nil, Nil, Nil, Set.empty, builtInConnectionKinds, Nil, Map.empty, Nil)
    for {
      afterBuiltins <- builtins.foldLeft(Resource.pure[F, Acc[F]](start))((acc, b) => acc.flatMap(a => builtin(b, context, a)))
      dirs <- Resource.eval(F.blocking(subdirectories(settings.dir)))
      manifests <- Resource.eval(dirs.traverse(d => F.blocking(readManifest(d)).map(d -> _)))
      // Newest first, so that when two majors of a plugin declare the same kind of connection, the newest keeps it.
      ordered = manifests.sortBy { case (d, m) => (m.map(_.name).getOrElse(""), m.map(x => -x.version.major).getOrElse(0), d.getFileName.toString) }
      builtinNames = builtins.map(_.name).toSet
      results <- ordered.foldLeft(Resource.pure[F, Acc[F]](afterBuiltins)) { case (acc, (dir, manifest)) =>
        acc.flatMap(a => one(settings.dir, dir, manifest, context, reserved, builtinNames, log, a))
      }
      loaded = Loaded(results.nodes, results.connectionKinds, results.statuses.reverse, results.entities, results.triggers)
      _ <- Resource.eval(
        F.raiseError[Unit](new IllegalStateException(failures(loaded))).whenA(settings.strict && loaded.failed.nonEmpty)
      )
    } yield loaded
  }

  /** Installs a plugin of the server; whatever is wrong with it stops the start. */
  private def builtin[F[_]](b: Builtin, context: FlowPlugin.Context[F], acc: Acc[F])(implicit F: Async[F]): Resource[F, Acc[F]] = {
    def fail(problem: String) = Resource.raiseError[F, Acc[F], Throwable](Problem(s"built-in plugin ${b.name}: $problem"))
    b.plugin.install[F](context).flatMap { installed =>
      val nodes = installed.nodes
      val conns = b.plugin.connectionKinds
      val entities = b.plugin.entities
      val kinds = nodes.map(_.descriptor.kind)
      val foreign = kinds.filterNot(k => k.startsWith(s"${b.name}."))
      val twice = kinds.diff(kinds.distinct).distinct ++ kinds.filter(acc.nodes.map(_.descriptor.kind).toSet)
      val taken = conns.map(_.kind).filter(acc.taken.contains)
      if (acc.ids.contains(b.name)) fail("installed twice")
      else if (foreign.nonEmpty) fail(s"kinds must start with '${b.name}.': ${foreign.mkString(", ")}")
      else if (twice.nonEmpty) fail(s"kinds declared twice: ${twice.distinct.mkString(", ")}")
      else if (taken.nonEmpty) fail(s"kinds of connection that already exist: ${taken.mkString(", ")}")
      else
        checkEntities(b.name, entities, acc.owners) match {
          case Some(problem) => fail(problem)
          case None =>
            val manifest = PluginManifest(b.name, BuiltinVersion, BuiltinVersion, Transport.Jvm)
            val status = PluginStatus(b.name, Some(manifest), State.Loaded(kinds, conns.map(_.kind), entities.map(_.name), installed.triggers.map(_.kind).distinct), builtin = true)
            Resource.pure[F, Acc[F]](
              Acc(
                acc.nodes ++ nodes,
                acc.connectionKinds ++ conns,
                status :: acc.statuses,
                acc.ids + b.name,
                acc.taken ++ conns.map(_.kind),
                acc.entities ++ entities.map(b.name -> _),
                acc.owners ++ entities.map(_.name -> b.name),
                acc.triggers ++ installed.triggers
              )
            )
        }
    }
  }

  /** The version the built-in plugins show: the plugin API of the server, which they always match. */
  private val BuiltinVersion: PluginManifest.Version = PluginManifest.Version.parse(FlowPlugin.ApiVersion).fold(e => sys.error(e), identity)

  /** What was loaded so far (`statuses` in reverse); `taken`, the kinds of connection that already have an owner; `owners`, the
    * name of the plugin that brought each entity.
    */
  private final case class Acc[F[_]](
      nodes: List[NodeType[F]],
      connectionKinds: List[ConnectionKind],
      statuses: List[PluginStatus],
      ids: Set[String],
      taken: Set[String],
      entities: List[(String, EntityDef)],
      owners: Map[String, String],
      triggers: List[TriggerSource[F]]
  ) {
    def failed(status: PluginStatus): Acc[F] = copy(statuses = status :: statuses)
  }

  private def one[F[_]](
      root: Path,
      dir: Path,
      manifest: Either[String, PluginManifest],
      context: FlowPlugin.Context[F],
      builtIn: Set[String],
      builtinNames: Set[String],
      log: String => F[Unit],
      acc: Acc[F]
  )(implicit
      F: Async[F]
  ): Resource[F, Acc[F]] = {
    val rel = root.relativize(dir).toString
    def fail(m: Option[PluginManifest], reason: String) = Resource.pure[F, Acc[F]](acc.failed(PluginStatus(rel, m, State.Failed(reason))))

    manifest match {
      case Left(reason) => fail(None, reason)
      case Right(m) if m.apiVersion.major != ApiMajor =>
        fail(Some(m), s"built for the plugin API ${m.apiVersion}; this server has ${FlowPlugin.ApiVersion}")
      case Right(m) if builtinNames.contains(m.name) => fail(Some(m), s"'${m.name}' is the name of a plugin of the server")
      case Right(m) if acc.ids.contains(m.id) => fail(Some(m), s"${m.id} is installed twice")
      case Right(m) =>
        val loaded = m.transport match {
          case Transport.Jvm     => jvm(dir, m, context)
          case Transport.Process => ProcessPlugin.load(dir, m, context.connections, line => log(s"[${m.id}] $line")).map { case (n, c, e) => (n, c, e, List.empty[TriggerSource[F]]) }
        }
        loaded.map { case (nodes, conns, entities, triggers) => (nodes.map(rename(m, _)), conns, entities, triggers) }.attempt.map {
          case Left(e) => acc.failed(PluginStatus(rel, Some(m), State.Failed(reason(e)))).copy(ids = acc.ids + m.id)
          case Right((nodes, conns, entities, triggers)) =>
            check(m, nodes, conns, builtIn).orElse(checkEntities(m.name, entities, acc.owners)) match {
              case Some(problem) => acc.failed(PluginStatus(rel, Some(m), State.Failed(problem))).copy(ids = acc.ids + m.id)
              case None =>
                // A kind of connection or an entity already declared by a newer major of the same plugin is not declared again.
                val fresh = conns.filterNot(c => acc.taken.contains(c.kind))
                val freshEntities = entities.filterNot(e => acc.owners.contains(e.name))
                val status = PluginStatus(rel, Some(m), State.Loaded(nodes.map(_.descriptor.kind), conns.map(_.kind), entities.map(_.name), triggers.map(_.kind).distinct))
                Acc(
                  acc.nodes ++ nodes,
                  acc.connectionKinds ++ fresh,
                  status :: acc.statuses,
                  acc.ids + m.id,
                  acc.taken ++ fresh.map(_.kind),
                  acc.entities ++ freshEntities.map(m.id -> _),
                  acc.owners ++ freshEntities.map(_.name -> m.name),
                  acc.triggers ++ triggers
                )
            }
        }
    }
  }

  /** The nodes and the kinds of connection of a `jvm` plugin. */
  private def jvm[F[_]](dir: Path, m: PluginManifest, context: FlowPlugin.Context[F])(implicit
      F: Async[F]
  ): Resource[F, (List[NodeType[F]], List[ConnectionKind], List[EntityDef], List[TriggerSource[F]])] =
    for {
      jars <- Resource.eval(F.blocking(jarsOf(dir)))
      _ <- Resource.eval(F.raiseError[Unit](Problem("no jar in the plugin's directory")).whenA(jars.isEmpty))
      loader <- Resource.fromAutoCloseable(F.delay(new PluginClassLoader(m.id, jars.map(_.toUri.toURL).toArray, classOf[FlowPlugin].getClassLoader)))
      plugins <- Resource.eval(F.blocking {
        ServiceLoader.load(classOf[FlowPlugin], loader).stream().toScala(List).filter(_.`type`().getClassLoader eq loader).map(_.get())
      })
      _ <- Resource.eval(F.raiseError[Unit](Problem(s"no ${classOf[FlowPlugin].getName} in META-INF/services")).whenA(plugins.isEmpty))
      installed <- plugins.traverse(_.install[F](context))
    } yield (installed.flatMap(_.nodes), plugins.flatMap(_.connectionKinds), plugins.flatMap(_.entities), installed.flatMap(_.triggers))

  /** Problems with what a plugin declares, or none. */
  private def check[F[_]](m: PluginManifest, nodes: List[NodeType[F]], conns: List[ConnectionKind], builtIn: Set[String]): Option[String] = {
    val kinds = nodes.map(_.descriptor.kind)
    val badKind = nodes.map(n => original(m, n.descriptor.kind)).find(k => !KindRe.matches(k))
    val twice = kinds.diff(kinds.distinct).distinct
    val notOwn = conns.map(_.kind).filterNot(k => k == m.name || k.startsWith(s"${m.name}."))
    val builtInClash = conns.map(_.kind).filter(builtIn.contains)
    badKind
      .map(k => s"kind '$k' must be letters, digits, '_' and '-', starting with a lower case letter")
      .orElse(Option.when(twice.nonEmpty)(s"kinds declared twice: ${twice.mkString(", ")}"))
      .orElse(Option.when(notOwn.nonEmpty)(s"kinds of connection must be '${m.name}' or start with '${m.name}.': ${notOwn.mkString(", ")}"))
      .orElse(Option.when(builtInClash.nonEmpty)(s"kinds of connection of the server: ${builtInClash.mkString(", ")}"))
  }

  /** Entities of a plugin that are named twice, or that another plugin brought already (one of the same name is a newer major of
    * it, whose entities are kept).
    */
  private def checkEntities(plugin: String, entities: List[EntityDef], owners: Map[String, String]): Option[String] = {
    val names = entities.map(_.name)
    val twice = names.diff(names.distinct).distinct
    val others = names.flatMap(n => owners.get(n).filter(_ != plugin).map(o => s"'$n' (plugin $o)"))
    Option.when(twice.nonEmpty)(s"entities declared twice: ${twice.mkString(", ")}")
      .orElse(Option.when(others.nonEmpty)(s"entities that another plugin brings: ${others.mkString(", ")}"))
  }

  private def original(m: PluginManifest, full: String): String = full.stripPrefix(s"${m.name}.").stripSuffix(s"@${m.version.major}")

  /** The same node under its full name, `<plugin>.<kind>@<major>`, receiving its params already resolved: every
    * `${state.x}` / `${context.key}` reference of [[Template]] is replaced by its value before the node runs, so a plugin
    * never has to know the syntax. The validator learns the paths those references read and their mistakes.
    */
  private def rename[F[_]](m: PluginManifest, node: NodeType[F])(implicit F: Async[F]): NodeType[F] = {
    val original = node.descriptor
    val d: NodeDescriptor = original.copy(
      kind = m.kindName(original.kind),
      reads = p => (original.reads(p) ++ Template.reads(p).map(StateRead(_, DataType.AnyJson))).distinctBy(_.path),
      checkParams = p => original.checkParams(p) ++ Template.problems(p)
    )
    def resolved(input: NodeInput): Either[String, NodeInput] = Template.resolve(input.params, input).map(p => input.copy(params = p))
    new NodeType[F] {
      val descriptor: NodeDescriptor = d
      def run(input: NodeInput): F[NodeOutput] = resolved(input).fold(e => F.pure(NodeOutput.failed(e)), node.run)
      override def simulated(input: NodeInput): Option[NodeOutput] = resolved(input).fold(e => Some(NodeOutput.failed(e)), node.simulated)
      override def unbounded: Boolean = node.unbounded
    }
  }

  private def subdirectories(dir: Path): List[Path] =
    if (!Files.isDirectory(dir)) Nil
    else {
      val s = Files.list(dir)
      try s.toScala(List).filter(d => Files.isDirectory(d) && Files.exists(d.resolve(PluginManifest.FileName))).sortBy(_.getFileName.toString)
      finally s.close()
    }

  private def jarsOf(dir: Path): List[Path] =
    List(dir, dir.resolve("lib")).filter(Files.isDirectory(_)).flatMap { d =>
      val s = Files.list(d)
      try s.toScala(List).filter(_.getFileName.toString.endsWith(".jar")).sortBy(_.getFileName.toString)
      finally s.close()
    }

  private def readManifest(dir: Path): Either[String, PluginManifest] =
    decode[PluginManifest](Files.readString(dir.resolve(PluginManifest.FileName))).leftMap(e => s"${PluginManifest.FileName}: ${e.getMessage}")

  /** A problem the loader found, whose message is enough; any other exception comes from the plugin and is named. */
  private[plugin] final case class Problem(message: String) extends RuntimeException(message)

  private def reason(e: Throwable): String = e match {
    case Problem(message) => message
    case _                => named(e)
  }

  private def named(e: Throwable): String = Option(e.getMessage).filter(_.nonEmpty).fold(e.getClass.getName)(msg => s"${e.getClass.getSimpleName}: $msg")

  def failures[F[_]](loaded: Loaded[F]): String =
    loaded.failed
      .map(s => s"${s.manifest.fold(s.dir)(_.id)}: ${s.state match { case State.Failed(r) => r; case _ => "" }}")
      .mkString("plugins that could not be loaded: ", "; ", "")
}
