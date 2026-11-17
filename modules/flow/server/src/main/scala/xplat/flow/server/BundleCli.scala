package xplat.flow.server

import cats.effect.{Async, ExitCode}
import cats.effect.std.Console
import cats.syntax.all._
import io.circe.Json
import io.circe.parser.parse
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{Method, Request, Status, Uri}
import xplat.flow.http.{Bundle, BundleFiles}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import scala.jdk.StreamConverters._

/** `flow-server.jar bundle ...`: moves flows between servers from a terminal or a CI job (see [[Bundle]]).
  *
  * {{{
  * bundle export --server http://dev:8080 [--flows a,b] (--dir flows-repo | --file bundle.json)
  * bundle plan   --server http://prod:8080 (--dir flows-repo | --file bundle.json)
  * bundle import --server http://prod:8080 (--dir flows-repo | --file bundle.json)
  * }}}
  *
  * `plan` and `import` end with code 1 when the bundle has problems, so a pipeline stops there.
  */
object BundleCli {

  val usage: String =
    """Usage: bundle <export|plan|import> --server <url> (--dir <directory> | --file <bundle.json>) [--flows a,b]
      |
      |  export   writes the flows of the server (or only --flows) with what they need: one file per item in --dir
      |           (the folders flows/, triggers/, entities/ and connections/ are replaced), or one JSON in --file
      |  plan     shows what importing would do on the server, without changing it; code 1 if it has problems
      |  import   imports it if it has no problems; code 1 otherwise
      |""".stripMargin

  final case class Args(command: String, server: Uri, dir: Option[Path], file: Option[Path], flows: List[String])

  def parseArgs(args: List[String]): Either[String, Args] = {
    def pairs(rest: List[String]): Either[String, Map[String, String]] = rest match {
      case Nil                                          => Right(Map.empty)
      case k :: v :: tail if k.startsWith("--")         => pairs(tail).map(_ + (k.drop(2) -> v))
      case other :: _                                   => Left(s"unexpected '$other'")
    }
    args match {
      case command :: rest if Set("export", "plan", "import").contains(command) =>
        for {
          opts <- pairs(rest)
          server <- opts.get("server").toRight("--server is needed").flatMap(s => Uri.fromString(s.stripSuffix("/")).leftMap(_ => s"not a URL: $s"))
          dir = opts.get("dir").map(Paths.get(_))
          file = opts.get("file").map(Paths.get(_))
          _ <- Either.cond(dir.isDefined != file.isDefined, (), "give one of --dir or --file")
          flows = opts.get("flows").toList.flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
        } yield Args(command, server, dir, file, flows)
      case _ => Left("the command is export, plan or import")
    }
  }

  def run[F[_]: Async: Console](args: List[String]): F[ExitCode] =
    parseArgs(args) match {
      case Left(problem) => Console[F].errorln(s"$problem\n\n$usage").as(ExitCode.Error)
      case Right(a)      => EmberClientBuilder.default[F].build.use(client => new Runner[F](client, a).run)
    }

  private final class Runner[F[_]](client: Client[F], a: Args)(implicit F: Async[F], C: Console[F]) {

    private val api = a.server / "api" / "bundle"

    def run: F[ExitCode] = a.command match {
      case "export" => exportBundle
      case "plan"   => send(dryRun = true)
      case _        => send(dryRun = false)
    }

    private def exportBundle: F[ExitCode] = {
      val uri = if (a.flows.isEmpty) api else api.withQueryParam("flows", a.flows.mkString(","))
      client.run(Request[F](Method.GET, uri)).use { r =>
        r.bodyText.compile.string.flatMap { body =>
          if (r.status != Status.Ok) C.errorln(s"${r.status}: $body").as(ExitCode.Error)
          else
            F.fromEither(parse(body).flatMap(_.as[Bundle]).leftMap(e => new IllegalStateException(s"the server did not answer a bundle: ${e.getMessage}"))).flatMap {
              bundle =>
                write(bundle) >> C.println(s"${bundle.flows.size} flows, ${bundle.triggers.size} triggers, ${bundle.entities.size} entities, " +
                  s"${bundle.connections.size} connections").as(ExitCode.Success)
            }
        }
      }
    }

    private def write(bundle: Bundle): F[Unit] = (a.dir, a.file) match {
      case (Some(dir), _) =>
        F.fromEither(BundleFiles.write(bundle).leftMap(new IllegalArgumentException(_))).flatMap { files =>
          F.blocking {
            // The folders of the items are the export's: what is not in the bundle any more goes.
            BundleFiles.Folders.map(dir.resolve).filter(Files.isDirectory(_)).foreach { folder =>
              val s = Files.list(folder)
              try s.toScala(List).filter(_.getFileName.toString.endsWith(".json")).foreach(Files.delete)
              finally s.close()
            }
            files.foreach { case (path, text) =>
              val target = dir.resolve(path)
              Files.createDirectories(target.getParent)
              Files.writeString(target, text, UTF_8)
            }
          }
        }
      case (_, Some(file)) =>
        F.blocking {
          Option(file.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
          Files.writeString(file, Bundle.printer.print(io.circe.Encoder[Bundle].apply(bundle)) + "\n", UTF_8)
        }.void
      case _ => F.unit
    }

    private def read: F[Bundle] = (a.dir, a.file) match {
      case (Some(dir), _) =>
        F.blocking {
          val s = Files.walk(dir)
          try s.toScala(List).filter(Files.isRegularFile(_)).map(p => dir.relativize(p).toString.replace('\\', '/') -> Files.readString(p, UTF_8)).toMap
          finally s.close()
        }.flatMap(files => F.fromEither(BundleFiles.read(files).leftMap(new IllegalArgumentException(_))))
      case (_, Some(file)) =>
        F.blocking(Files.readString(file, UTF_8)).flatMap(t => F.fromEither(parse(t).flatMap(_.as[Bundle]).leftMap(e => new IllegalArgumentException(s"$file: ${e.getMessage}"))))
      case _ => F.raiseError(new IllegalArgumentException("no bundle"))
    }

    private def send(dryRun: Boolean): F[ExitCode] =
      read.flatMap { bundle =>
        val uri = if (dryRun) api.withQueryParam("dryRun", "true") else api
        client.run(Request[F](Method.POST, uri).withEntity(io.circe.Encoder[Bundle].apply(bundle))).use { r =>
          r.bodyText.compile.string.flatMap { body =>
            parse(body).toOption.filter(_.hcursor.downField("items").succeeded) match {
              case Some(plan) => C.println(describe(plan, dryRun)).as(if (r.status == Status.Ok) ExitCode.Success else ExitCode.Error)
              case None       => C.errorln(s"${r.status}: $body").as(ExitCode.Error)
            }
          }
        }
      }.handleErrorWith(e => C.errorln(e.getMessage).as(ExitCode.Error))
  }

  /** A plan as lines for a terminal: what changes, then the problems and the warnings. */
  def describe(plan: Json, dryRun: Boolean): String = {
    val c = plan.hcursor
    val items = c.downField("items").as[List[Json]].getOrElse(Nil).map { i =>
      val h = i.hcursor
      (h.get[String]("action").getOrElse(""), h.get[String]("type").getOrElse(""), h.get[String]("id").getOrElse(""))
    }
    val problems = c.get[List[String]]("problems").getOrElse(Nil)
    val warnings = c.get[List[String]]("warnings").getOrElse(Nil)
    val changing = items.filterNot(_._1 == Bundle.Action.Same)
    val lines =
      changing.map { case (action, kind, id) => f"  $action%-7s $kind%-11s $id" } ++
        Option.when(changing.isEmpty)("  nothing changes").toList ++
        problems.map(p => s"  PROBLEM  $p") ++ warnings.map(w => s"  warning  $w")
    val head =
      if (problems.nonEmpty) "The bundle cannot be imported:"
      else if (dryRun) s"Importing would do (${items.count(_._1 == Bundle.Action.Same)} unchanged):"
      else s"Imported (${items.count(_._1 == Bundle.Action.Same)} unchanged):"
    (head :: lines).mkString("\n")
  }
}
