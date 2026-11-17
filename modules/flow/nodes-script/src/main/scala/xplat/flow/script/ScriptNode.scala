package xplat.flow.script

import cats.effect.Sync
import cats.syntax.all._
import io.circe.{Decoder, Json}
import xplat.flow.model._
import xplat.flow.nodes.NodeType

import scala.concurrent.duration._

/** A node whose body is JavaScript that the user writes in the editor.
  *
  * The code is the body of a function of `(state, memory, params, log)`:
  *
  * {{{
  * const total = state.order.items.reduce((sum, i) => sum + i.price * i.qty, 0);
  * log("total", total);
  * return { values: { total: total, big: total > 1000 }, port: "out", memory: { runs: (memory.runs || 0) + 1 } };
  * }}}
  *
  * `state` and `memory` are frozen copies, `params` is the `args` of the configuration, and `log` writes a line to the audit. It returns
  * an object (or nothing): `values` by the name of the outputs the node declares (the engine checks their types), `port` among the
  * ports of the node (`out` by default) and the new `memory`. A script that throws, runs too long or returns something else makes the
  * node fail through the `error` port, with the message and the line. It runs in the sandbox of [[ScriptSandbox]], with a context of its
  * own for each run: nothing is shared between runs or instances. Syntax is checked when the flow is saved.
  *
  * `files` are more files of the same script (`[{name, code}]`, such as `helpers.js`): they run before the code, in their order, so
  * what they declare at their top level the code can use. Their errors say the file and its line.
  */
final class ScriptNode[F[_]](limits: ScriptSandbox.Limits = ScriptSandbox.Limits())(implicit F: Sync[F]) extends NodeType[F] {

  val descriptor: NodeDescriptor = ScriptNode.descriptor

  def run(input: NodeInput): F[NodeOutput] =
    ScriptNode.params(input.params) match {
      case Left(problem) => NodeOutput.failed(problem).pure[F]
      case Right(p) =>
        F.blocking {
          ScriptSandbox.open(p.code, ScriptNode.Args, limits, p.files) match {
            case Left(error) => NodeOutput.failed(error.render)
            case Right(session) =>
              try {
                val timeout = p.timeoutMillis.fold(limits.timeout)(ms => ms.millis min limits.timeout)
                session.call(List(input.state, input.memory, p.args), timeout) match {
                  case Left(error)   => NodeOutput.failed(error.render)
                  case Right(result) => ScriptNode.output(p, result)
                }
              } finally session.close()
          }
        }
    }
}

object ScriptNode {

  val Kind: String = "script.js"

  /** The names the code sees, besides `log`. */
  val Args: List[String] = List("state", "memory", "params")

  /** @param code
    *   the body of the function.
    * @param ports
    *   the ports the node can leave through, besides `error`.
    * @param args
    *   a value for the script that is part of the configuration (it sees it as `params`).
    * @param timeoutMillis
    *   a shorter time limit than the server's.
    * @param files
    *   more files of the script, besides `code`.
    */
  final case class Params(
      code: String,
      ports: List[String] = List(Port.Out),
      args: Json = Json.obj(),
      timeoutMillis: Option[Long] = None,
      files: List[ScriptSandbox.File] = Nil
  )

  object Params {
    private implicit val fileDecoder: Decoder[ScriptSandbox.File] = Decoder.forProduct2("name", "code")(ScriptSandbox.File.apply)

    implicit val decoder: Decoder[Params] = Decoder.instance { c =>
      for {
        code <- c.get[String]("code")
        ports <- c.getOrElse[List[String]]("ports")(List(Port.Out))
        args <- c.getOrElse[Json]("args")(Json.obj())
        timeout <- c.get[Option[Long]]("timeoutMs")
        files <- c.getOrElse[List[ScriptSandbox.File]]("files")(Nil)
      } yield Params(code, ports, args, timeout, files)
    }
  }

  /** The type of `files`. */
  val FilesType: DataType = DataType.ListOf(DataType.Obj(Map("name" -> DataType.Str, "code" -> DataType.Str)))

  /** The field `files` of a script of this language, whose files end in `.extension`. */
  def filesField(language: String, extension: String): ConfigSchema.Field =
    ConfigSchema.Field(
      "files",
      FilesType,
      required = false,
      default = Some(Json.arr()),
      description = Some(s"More files of the script, such as helpers.$extension: [{name, code}]"),
      ui = Some(UiSpec(widget = Some("files"), language = Some(language)))
    )

  private val FileName = "[A-Za-z_][A-Za-z0-9_-]*".r

  /** What is wrong with the names of `files`: each is a simple name ending in `.extension`, once. */
  def fileProblems(files: List[ScriptSandbox.File], extension: String): List[String] = {
    val bad = files.map(_.name).filterNot(n => n.endsWith(s".$extension") && FileName.matches(n.stripSuffix(s".$extension"))).distinct
    val twice = files.map(_.name).diff(files.map(_.name).distinct).distinct
    bad.map(n => s"files: '$n' is not a name such as helpers.$extension (letters, digits, _ and -)") ++
      twice.map(n => s"files: '$n' is more than once")
  }

  /** The code a new node starts with. */
  val Template: String =
    """// state: the values of the flow; memory: what this node kept last time; params: its args.
      |// The files of the tabs run first: what they declare can be used here.
      |const items = state.order?.items ?? [];
      |const total = items.reduce((sum, i) => sum + i.price * i.qty, 0);
      |log("total", total);
      |
      |return {
      |  values: { total, big: total > 1000 },     // by the name of the outputs of the node
      |  port: "out",                              // one of its ports (see ports)
      |  memory: { runs: (memory.runs ?? 0) + 1 }, // what it sees as memory next time
      |};
      |""".stripMargin

  /** The outputs a new node declares: those that [[Template]] writes, so that it runs as it is. */
  val TemplateOutputs: List[OutputDecl] =
    List(OutputDecl("total", StatePath(List("total")), DataType.Num), OutputDecl("big", StatePath(List("big")), DataType.Bool))

  def params(json: Json): Either[String, Params] = json.as[Params].left.map(e => s"invalid configuration: ${e.message}")

  /** What the script returned, as the output of the node. */
  private[script] def output(p: Params, result: ScriptSandbox.Result): NodeOutput = {
    val info = if (result.logs.isEmpty) Map.empty[String, Json] else Map("logs" -> Json.arr(result.logs.map(Json.fromString): _*))
    result.value match {
      case None | Some(Json.Null) => NodeOutput(info = info)
      case Some(v) =>
        v.asObject match {
          case None => NodeOutput.failed("the script must return an object such as {values, port, memory}, or nothing").copy(info = info)
          case Some(o) =>
            val unknown = o.keys.filterNot(Set("values", "port", "memory")).toList.sorted
            val values = o("values").fold[Either[String, Map[String, Json]]](Right(Map.empty))(j => j.asObject.map(_.toMap).toRight("'values' must be an object"))
            val port = o("port").fold[Either[String, String]](Right(Port.Out))(j => j.asString.toRight("'port' must be a string"))
            (for {
              _ <- Either.cond(unknown.isEmpty, (), s"the script returned unknown fields: ${unknown.mkString(", ")}")
              vs <- values
              pt <- port
              _ <- Either.cond((p.ports :+ Port.Error).contains(pt), (), s"'$pt' is not a port of this node (${p.ports.mkString(", ")})")
            } yield NodeOutput(vs, pt, o("memory").filterNot(_.isNull), info = info)) match {
              case Right(out)   => out
              case Left(reason) => NodeOutput.failed(reason).copy(info = info)
            }
        }
    }
  }

  /** Problems of the configuration that need the code compiled: the syntax, and the ports. */
  private def check(json: Json): List[String] =
    params(json) match {
      case Left(problem) => List(problem)
      case Right(p) =>
        val ports =
          if (p.ports.isEmpty) List("ports must have at least one name")
          else if (p.ports.distinct.size != p.ports.size) List("ports has a name more than once")
          else if (p.ports.contains(Port.Error)) List("'error' is a port of every node already; leave it out of ports")
          else if (p.ports.exists(_.trim.isEmpty)) List("a port needs a name")
          else Nil
        val names = fileProblems(p.files, "js")
        val syntax = if (names.isEmpty) ScriptSandbox.check(p.code, Args, p.files).map(e => s"code: ${e.render}").toList else Nil
        ports ++ names ++ syntax
    }

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "Scripting",
    label = "Script (JavaScript)",
    description = "Runs JavaScript that reads the state and the memory of the node and answers values for its outputs and the port to go on through.",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("code", DataType.Str, description = Some("JavaScript: the body of a function of (state, memory, params, log). Return {values, port, memory}."), ui = Some(UiSpec.script("javascript", Template, "files"))),
        filesField("javascript", "js"),
        ConfigSchema.Field("ports", DataType.ListOf(DataType.Str), required = false, default = Some(Json.arr(Json.fromString(Port.Out))), description = Some("Ports the script can leave through, besides error")),
        ConfigSchema.Field("args", DataType.AnyJson, required = false, default = Some(Json.obj()), description = Some("A value the script sees as `params`")),
        ConfigSchema.Field("timeoutMs", DataType.Num, required = false, description = Some("Time limit in milliseconds; it cannot exceed the server's"))
      )
    ),
    sideEffects = SideEffects.Pure,
    defaultOutputs = _ => TemplateOutputs,
    ports = json => params(json).toOption.map(_.ports.distinct.filter(_.nonEmpty).map(Port(_))).filter(_.nonEmpty).getOrElse(List(Port(Port.Out))),
    checkParams = check
  )
}
